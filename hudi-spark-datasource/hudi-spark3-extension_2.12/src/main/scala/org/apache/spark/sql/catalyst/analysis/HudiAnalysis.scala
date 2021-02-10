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

package org.apache.spark.sql.catalyst.analysis

import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.execution.HudiSQLUtils
import org.apache.hudi.execution.HudiSQLUtils.HoodieV1Relation
import org.apache.hudi.spark3.internal.HoodieDataSourceInternalTable
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Literal}
import org.apache.spark.sql.catalyst.merge.HudiMergeIntoUtils
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable

/**
 * Analysis rules for hudi. deal with * in merge clause and insert into clause
 */
class HudiAnalysis(session: SparkSession, conf: SQLConf) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    case dsv2 @ DataSourceV2Relation(d: HoodieDataSourceInternalTable, _, _, _, options) =>
      HoodieV1Relation.fromV2Relation(dsv2, d, options)
    // ResolveReferences rule will resolve * in merge, but the resolve result is not what we expected
    // so if source is not resolved, we add a special placeholder to prevert ResolveReferences rule to resolve * in mergeIntoTable
    case m @ MergeIntoTable(target, source, _, _, _) =>
      if (source.resolved) {
        null
      } else {
        null
      }
    // we should deal with Meta columns in hudi, it will be safe to deal insertIntoStatement here.
    case i @ InsertIntoStatement(table, _, query, _, _)
      if table.resolved && query.resolved && HudiSQLUtils.isHudiRelation(table) =>
      table match {
        case dsv2 @ DataSourceV2Relation(d: HoodieDataSourceInternalTable, _, _, _, _) =>
          preprocessV2(i, d, query)
        case other => other
      }
  }

  /**
    * do align hoodie metacols, hoodie tablehave metaCols which are hidden by default, we try our best to fill those cols auto
    */
  private def preprocessV2(insert: InsertIntoStatement, d: HoodieDataSourceInternalTable, query: LogicalPlan): InsertIntoStatement = {
    val tableSchema = d.schema()
    val querySchema = query.schema
    val hoodieMetaCols = tableSchema.collect { case p if HoodieRecord.HOODIE_META_COLUMNS.contains(p.name) => p }
    val queryFakeMetaCols = querySchema.collect { case p if HoodieRecord.HOODIE_META_COLUMNS.contains(p.name) => p }

    val staticPartitionsNumber = insert.partitionSpec.filter(_._2.isDefined).size

    if (querySchema.size + staticPartitionsNumber + hoodieMetaCols.size == tableSchema.size
      && queryFakeMetaCols.size == 0 && hoodieMetaCols.size > 0) {
      // do align
      val filled  = hoodieMetaCols.map { f =>
        Alias(Literal.create(null, f.dataType), f.name)()
      }
      insert.copy(query = Project(filled ++ query.output, query))
    } else {
      insert
    }
  }

  private def placeHolderStarAction(m: MergeIntoTable): MergeIntoTable = {
    val MergeIntoTable(target, source, condition, matched, notMatched) = m
    val matchdActions = matched.map {
      case update: UpdateAction if update.assignments.isEmpty =>
        val newAssignment = Seq(Assignment(Literal.FalseLiteral, Literal.FalseLiteral))
        update.copy(update.condition, newAssignment)
      case other => other
    }
    val noMatchedActions = notMatched.map {
      case insert: InsertAction =>
        if (insert.assignments.isEmpty) {
          val newAssignment = Seq(Assignment(Literal.FalseLiteral, Literal.FalseLiteral))
          insert.copy(insert.condition, newAssignment)
        } else {
          insert
        }
    }
    m.copy(target, source, condition, matchdActions, noMatchedActions)
  }

  private def dealWithStarAction(m: MergeIntoTable): MergeIntoTable = {
    val MergeIntoTable(target, source, condition, matched, notMatched) = m
    val neededSchema = {
      val migratedSchema = mutable.ListBuffer[StructField]()
      target.schema.foreach(migratedSchema.append(_))
      source.schema.filterNot { col =>
        target.schema.exists(targetCol => target.conf.resolver(targetCol.name, col.name))
      }.foreach(migratedSchema.append(_))
      StructType(migratedSchema)
    }

    // deal with * which marked by a special placeHolder
    val matchedActions = matched.map {
      case update: UpdateAction
        if ((update.assignments.length == 1 && update.assignments(0).key.isInstanceOf[Literal])
          || update.assignments.isEmpty) =>
        val newAssignment = neededSchema.map { col =>
          val expr = source.output.find { a =>
            conf.resolver(a.name, col.name)
          }.orElse {
            target.output.find(a => conf.resolver(a.name, col.name))
          }.getOrElse {
            throw new AnalysisException(s"cannot expand ${col.name} for updateAction")
          }
          Assignment(UnresolvedAttribute(col.name), expr)
        } ++ Seq(Assignment(Literal.TrueLiteral, Literal.TrueLiteral))
        update.copy(update.condition, newAssignment)
      case insert: InsertAction =>
        throw new AnalysisException(s"insert clauses: ${insert.toString()} cannnot be a part of the when Matched")
      case other => other
    }

    val noMatchedActions = notMatched.map {
      case insert: InsertAction =>
        if ((insert.assignments.length == 1 && insert.assignments(0).key.isInstanceOf[Literal])
        || insert.assignments.isEmpty) {
        val newAssignment = neededSchema.map { col =>
          val expr = source.output.find { a =>
            conf.resolver(a.name, col.name)
          }
          if (expr.isEmpty) {
            Assignment(UnresolvedAttribute(col.name), Alias(Literal.create(null, col.dataType), col.name)())
          } else {
            // extra col
            val resolvedCol = HudiMergeIntoUtils.tryResolveReferences(session)(UnresolvedAttribute(col.name), Project(source.output, source))
            Assignment(resolvedCol, expr.get)
          }
        } ++ Seq(Assignment(Literal.TrueLiteral, Literal.TrueLiteral))
        insert.copy(insert.condition, newAssignment)
      } else insert
      case _ =>
        throw new AnalysisException(s"only insert clause can be a part of the when Not Matched")
    }
    m.copy(target, source, condition, matchedActions, noMatchedActions)
  }
}
