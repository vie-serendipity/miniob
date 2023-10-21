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
// Created by Wangyunlai on 2022/5/22.
//

#pragma once

#include "common/rc.h"
#include "storage/field/field.h"
#include "sql/stmt/stmt.h"

class Table;
class FilterStmt;
class Db;

/**
 * @brief 更新语句
 * @ingroup Statement
 */
class UpdateStmt : public Stmt 
{
public:
  UpdateStmt() = default;
  UpdateStmt(Table *table, const Value *values, int value_amount, FilterStmt *filter_stmt);
  ~UpdateStmt() override;

public:
  static RC create(Db *db, const UpdateSqlNode &update_sql, Stmt *&stmt);

public:
  Table *table() const
  {
    return table_;
  }
  const Value *values() const
  {
    return values_;
  }
  int value_amount() const
  {
    return value_amount_;
  }
  const std::vector<Field> &fields() const
  {
    return update_fields_;
  }
  FilterStmt *filter_stmt() const
  {
    return filter_stmt_;
  }
  StmtType type() const override
  {
    return StmtType::UPDATE;
  }

private:
  Table *table_ = nullptr;
  const Value *values_ = nullptr;
  std::vector<Field> update_fields_;
  int value_amount_ = 0;
  FilterStmt *filter_stmt_ = nullptr;
};
