/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.execution

import org.apache.hudi.exception.HoodieException
import org.apache.hudi.DataSourceWriteOptions.{DEFAULT_PAYLOAD_OPT_VAL, PAYLOAD_CLASS_OPT_KEY}
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload
import org.apache.hudi.execution.HudiSQLUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.merge._
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, Expression, Literal, PredicateHelper, UnsafeProjection}
import org.apache.spark.sql.functions.{col, lit, udf, when}
import org.apache.spark.sql.catalyst.merge.{HudiMergeClause, HudiMergeInsertClause}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.{StringType, StructType}

import scala.collection.mutable.ArrayBuffer

/**
 * merge command, execute merge operation for hudi
 */
case class MergeIntoHudiTable(
    target: LogicalPlan,
    source: LogicalPlan,
    joinCondition: Expression, matchedClauses: Seq[HudiMergeClause],
    noMatchedClause: Option[HudiMergeInsertClause],
    finalSchema: StructType,
    canMergeDirectly: Boolean = false,
    trimmedSchema: StructType) extends RunnableCommand with Logging with PredicateHelper {

  lazy val tableMeta: Map[String, String] = {
    HudiSQLUtils.getHoodiePropsFromRelation(target)
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (canMergeDirectly) {
      doMergeDirectly(sparkSession)
    } else {
      doMergeNormal(sparkSession)
    }
    Seq.empty[Row]
  }

  /**
   * do merge directly, just like hoodie dataFrame api, better performance
   */
  private def doMergeDirectly(sparkSession: SparkSession): Unit = {
    logInfo("do merging")
    val df = Dataset.ofRows(sparkSession, source)
    val savaPath = tableMeta.getOrElse("path", throw new HoodieException("cannnot get table path, pls check it"))

    val payload = HudiSQLUtils.getPropertiesFromTableConfigCache(savaPath)
      .getProperty(PAYLOAD_CLASS_OPT_KEY, DEFAULT_PAYLOAD_OPT_VAL)

    val rawDataFrame: DataFrame = payload match {
      case HudiSQLUtils.AWSDMSAVROPAYLOAD =>
        if (source.schema.exists(p => p.name.equals("Op"))) {
          // if people use AWSDmsAvroPayload, the UID operation is already assign by "Op" field
          // so just ignore all the merge condition
          df.withColumn(HudiSQLUtils.MERGE_MARKER, col("Op"))
        } else {
          throw new HoodieException("payload is AWSDmsAvroPayload but cannot find Op field in source ")
        }
      case _ =>
        df.withColumn(HudiSQLUtils.MERGE_MARKER, combineClauseConditions())
          .filter(s"${HudiSQLUtils.MERGE_MARKER} != 'O")
    }
    HudiSQLUtils.merge(rawDataFrame, sparkSession, tableMeta, false)
    logInfo("merge finished")
  }

  /**
   * trim updateClause, which will reduce the data size in memory
   * when trimming condition is satisfied
   * we no need unnecessary column for update, just set them null,
   * hudi will deal with null value itself
   */
  private def doTrimMergeUpdateClause(): Seq[HudiMergeClause] = {
    val unWantedTargetColumns = target.schema.filter(p => trimmedSchema.find(t => conf.resolver(p.name, t.name)).isDefined)
    val trimmedMatchedClauses = matchedClauses.map {
      case u: HudiMergeUpdateClause =>
        val trimActions = u.resolvedActions.map { act =>
          if (unWantedTargetColumns.exists(p => conf.resolver(p.name, act.targetColNameParts.head))) {
            act.copy(act.targetColNameParts, Literal(null, act.expr.dataType))
          } else {
            act
          }
        }
        u.copy(u.condition, trimActions)
      case other => other
    }
    trimmedMatchedClauses
  }

  /**
   * normal deleteClause output, which will reduce the data size in memory
   * we just keep necessary column, otherwise set them to null
   */
  private def doNormalMergeDeleteClause(): Seq[Expression] = {
    val savePath = tableMeta.get("path").get
    val (combineKey, _) = HudiSQLUtils.getRequredFieldsFromTableConf(savePath)
    val specialColumn = Set[String]("_hoodie_commit_time", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name", combineKey)

    finalSchema.map { f =>
      if (specialColumn.contains(f.name)) {
        target.resolve(Seq(f.name), conf.resolver).get
      } else {
        Literal(null, f.dataType)
      }
    }
  }

  /**
   * now only OverwriteNonDefaultsWithLatestAvroPayload allow trim
   */
  private def canTrim(): Boolean = {
    val tablePath = tableMeta.get("path").get
    val payload = HudiSQLUtils.getPropertiesFromTableConfigCache(tablePath).getProperty(PAYLOAD_CLASS_OPT_KEY, DEFAULT_PAYLOAD_OPT_VAL)

    if (payload.equals(classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName)) {
      true
    } else {
      false
    }
  }

  /**
   * do merge Normal
   * step1: select join type
   * step2: merge all clause condition
   * step3: build joinPlan and add _hoodie_is_delete field to mark delete rows
   * step4: build mapPartitition function to get final result for merging
   */
  private def doMergeNormal(sparkSession: SparkSession): Unit = {
    val doTrim = if (canTrim() && trimmedSchema != null) true else false

    val containsMatchClauses = matchedClauses.isEmpty
    val containsNoMatchedClause = noMatchedClause.isEmpty
    val joinTye: String = {
      if (!containsNoMatchedClause && containsMatchClauses) {
        "inner"
      } else  {
        "right_outer"
      }
    }

    val trimmedMatchedClauses = if (doTrim) doTrimMergeUpdateClause() else matchedClauses

    val joinDF = buildJoinDataFrame(sparkSession, joinTye, doTrim)

    logInfo(
      s"""
         |build join with:
         |targetPlan: ${target.toString()}
         |sourcePlan: ${source.toString()}
         | joinType: ${joinTye}
         | doTrimTarget: ${doTrim}
       """.stripMargin)

    val joinPlan = joinDF.queryExecution.analyzed
    val fSchema = finalSchema.add(HudiSQLUtils.MERGE_MARKER, StringType, true)

    def resolveOnJoinPlan(exprSeq: Seq[Expression]): Seq[Expression] = {
      exprSeq.map { expr => HudiMergeIntoUtils.tryResolveReferences(sparkSession)(expr, joinPlan)}
    }

    def createClauseOutput(clause: HudiMergeClause): Seq[Expression] = {
      val resolveExprSeq = clause match {
        case u: HudiMergeUpdateClause =>
          u.resolvedActions.map(_.expr) :+ joinPlan.resolve(Seq(HudiSQLUtils.MERGE_MARKER), conf.resolver).get
        case i: HudiMergeInsertClause =>
          i.resolvedActions.map(_.expr) :+ joinPlan.resolve(Seq(HudiSQLUtils.MERGE_MARKER), conf.resolver).get
        case _: HudiMergeDeleteClause =>
          doNormalMergeDeleteClause() :+ joinPlan.resolve(Seq(HudiSQLUtils.MERGE_MARKER), conf.resolver).get
      }
      resolveOnJoinPlan(resolveExprSeq)
    }

    val updateClause = trimmedMatchedClauses.find(_.isInstanceOf[HudiMergeUpdateClause]).headOption
    val deleteClause = trimmedMatchedClauses.find(_.isInstanceOf[HudiMergeDeleteClause]).headOption
    val insertClause = noMatchedClause
    val joinedRowEncoder = RowEncoder(joinPlan.schema)
    val finalOutputEncoder = RowEncoder(fSchema).resolveAndBind()
    val uidProcess = new MergeIntoCommand.UIDProcess(
      updateClause.map(createClauseOutput(_)),
      deleteClause.map(createClauseOutput(_)),
      insertClause.map(createClauseOutput(_)),
      joinPlan.output,
      joinedRowEncoder,
      finalOutputEncoder)

    val rawDataFrame = Dataset.ofRows(sparkSession, joinPlan).mapPartitions(uidProcess.processUID(_))(finalOutputEncoder)

    logInfo(s"construct merge data: ${rawDataFrame.schema.toString()} for hudi finished, start merge .....")

    HudiSQLUtils.merge(rawDataFrame, sparkSession, tableMeta, true)

  }

  /**
   * buildJoinDataFrame
   * add "exsit_on_target" column to mark the row belong to target
   * add "exsit_on_source" column to mark the row belong to source
   */
  private def buildJoinDataFrame(sparkSession: SparkSession, joinType: String, doTrim: Boolean = false): DataFrame = {
    val trimmedTarget = if (doTrim) {
      Project(target.output.filter { col => trimmedSchema.exists(p => conf.resolver(p.name, col.name))}, target)
    } else {
      target
    }
    // try to push some join filter
    val targetPredicates = splitConjunctivePredicates(joinCondition)
      .find(p => p.references.subsetOf(trimmedTarget.outputSet)).reduceLeftOption(And)

    val targetDF = Dataset.ofRows(sparkSession, trimmedTarget).withColumn("exist_on_target", lit(true))
    val sourceDF = Dataset.ofRows(sparkSession, source).withColumn("exist_on_source", lit(true))

    targetPredicates.map(f => targetDF.filter(Column(f))).getOrElse(targetDF)
      .join(sourceDF, new Column(joinCondition), joinType)
      .withColumn(HudiSQLUtils.MERGE_MARKER, null)
  }

  /**
   * combine all clause conditions
   * build clause condition "_hoodie_is_deleted"
   * case 1 = 1
   * when isnotnull('exsit_on_target') && isnotnull('exsit_on_source') && updateCondition then 'U'
   * when isnotnull('exsit_on_target') && isnotnull('exsit_on_source') && deleteCondition then 'D'
   * when isnull('exsit_on_target') && isnotnull('exsit_on_source') && insertCondtition then 'I'
   * no need to consider when isnotnull('exsit_on_target') && isnull('exsit_on_source') && insertCondtition then 'O' since hudi has pk
   * else 'O'
   * end
   */
  private def combineClauseConditions(): Column = {
    val columnList = new ArrayBuffer[(Column, String)]()
    matchedClauses.foreach {
      case m: HudiMatchedClause =>
        var column: Column = if (m.condition.isDefined) {
          new Column(m.condition.get)
        } else null
        val matchedColumn = col("exist_on_target").isNotNull.and(col("exist_on_source").isNotNull)
        if (column == null) {
          column = matchedColumn
        } else {
          column = column.and(matchedColumn)
        }
        if (m.isInstanceOf[HudiMergeUpdateClause]) {
          columnList.append((column, "U"))
        } else if (m.isInstanceOf[HudiMergeDeleteClause]) {
          columnList.append((column, "D"))
        }
      case _ =>
    }

    noMatchedClause.toSeq.foreach {
      case i: HudiMergeInsertClause =>
        var column: Column = if (i.condition.isDefined) {
          new Column(i.condition.get)
        } else null
        val matchedColumn = col("exist_on_target").isNull.and(col("exist_on_source").isNotNull)
        if (column == null) {
          column = matchedColumn
        } else {
          column = column.and(matchedColumn)
        }
        columnList.append((column, "I"))
    }

    var mergeCondition: Column = null
    columnList.foreach { case (column, index) =>
      if (mergeCondition == null) {
        mergeCondition = when(column, lit(index))
      } else {
        mergeCondition = mergeCondition.when(column, lit(index))
      }
    }
    mergeCondition.otherwise(lit("O"))
  }
}

object MergeIntoCommand {
  class UIDProcess(
      updateOutput: Option[Seq[Expression]],
      deleteOutput: Option[Seq[Expression]],
      insertOutput: Option[Seq[Expression]],
      joinAttributes: Seq[Attribute],
      joinedRowEncoder: ExpressionEncoder[Row],
      finalOutputRowEncoder: ExpressionEncoder[Row]) extends Serializable {

    def processUID(rowIterator: Iterator[Row]): Iterator[Row] = {
      val toRow = joinedRowEncoder.createSerializer()
      val fromRow = finalOutputRowEncoder.createDeserializer()
      val updateProject = updateOutput.map(UnsafeProjection.create(_, joinAttributes))
      val insertProject = insertOutput.map(UnsafeProjection.create(_, joinAttributes))
      val deleteProject = deleteOutput.map(UnsafeProjection.create(_, joinAttributes))

      rowIterator.map(toRow).filterNot { irow =>
        irow.getString(joinAttributes.length - 1) == "O"
      }.map { irow =>
        irow.getString(joinAttributes.length - 1) match {
          case "U" =>
            updateProject.get.apply(irow)
          case "I" =>
            insertProject.get.apply(irow)
          case "D" =>
            deleteProject.get.apply(irow)
        }
      }.map {irow =>
        val finalProject = UnsafeProjection.create(finalOutputRowEncoder.schema)
        fromRow(finalProject(irow))
      }
    }
  }
}
