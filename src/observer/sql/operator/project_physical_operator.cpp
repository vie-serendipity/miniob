/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/07/01.
//

#include "common/log/log.h"
#include "sql/operator/project_physical_operator.h"
#include "storage/record/record.h"
#include "storage/table/table.h"

RC ProjectPhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  PhysicalOperator *child = children_[0].get();
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC ProjectPhysicalOperator::next()
{
  if (aggregations_.empty()) {
    if (children_.empty()) {
      return RC::RECORD_EOF;
    }
    return children_[0]->next();
  } else {
    // aggregation
    RC rc = RC::SUCCESS;
    if (emitted_) {
      rc = RC::RECORD_EOF;
      return rc;
    }
    emitted_ = true;

    while (RC::SUCCESS == children_[0]->next()) {
      Tuple* tuple = children_[0]->current_tuple();
      if (nullptr == tuple) {
        rc = RC::INTERNAL;
        LOG_WARN("failed to get tuple from operator");
        break;
      }

      Value value, one(1);
      
      for (int i = 0; i < aggregations_.size(); i++) {
        auto& agg = aggregations_[i];
        if (agg.agg_fun == AggFun::AGG_COUNT) {
          if (agg.attribute_name == "*" || agg.attribute_name == "1") {
            rc = tuple->cell_at(0, value);
          } else {
            rc = tuple->find_cell(TupleCellSpec(agg.relation_name.c_str(), agg.attribute_name.c_str()), value);
          }
          if (rc != RC::SUCCESS) {
            LOG_WARN("failed to find cell: %s", strrc(rc));
            continue;
          }
          
          if (tuple_.cell_type(i) == AttrType::UNDEFINED) {
            tuple_.initialize(i, value.attr_type());
          }
          tuple_.add(i, one);
        } else {
          rc = tuple->find_cell(TupleCellSpec(agg.relation_name.c_str(), agg.attribute_name.c_str()), value);
          if (rc != RC::SUCCESS) {
            LOG_WARN("failed to find cell: %s", strrc(rc));
            break;
          }
          if (tuple_.cell_type(i) == AttrType::UNDEFINED) {
            tuple_.initialize(i, value.attr_type());
          }
          tuple_.add(i, value);
        }
      }
    }
    tuple_.finish();
    return rc;
  }

}

RC ProjectPhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}

Tuple *ProjectPhysicalOperator::current_tuple()
{
  if (aggregations_.empty()) {
    project_tuple_.set_tuple(children_[0]->current_tuple());
    return &project_tuple_;
  } else {
    return &tuple_;
  }
}

void ProjectPhysicalOperator::add_projection(const Table *table, const FieldMeta *field_meta)
{
  // 对单表来说，展示的(alias) 字段总是字段名称，
  // 对多表查询来说，展示的alias 需要带表名字
  TupleCellSpec *spec = new TupleCellSpec(table->name(), field_meta->name(), field_meta->name());
  project_tuple_.add_cell_spec(spec);
}
