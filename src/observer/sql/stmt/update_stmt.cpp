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

#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"

UpdateStmt::UpdateStmt(Table *table, Value *values, int value_amount)
    : table_(table), values_(values), value_amount_(value_amount)
{}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  // TODO
  stmt = nullptr;
  // const char *table_name = update.relation_name.c_str();
  // if (nullptr==db || nullptr == table_name ) {
  //   LOG_WARN("invalid argument. db=%p, table_name=%p", db, table_name);
  //   return RC::INVALID_ARGUMENT;
  // }

  // // check whether the table exists
  // Table *table = db->find_table(table_name);
  // if (nullptr == table){
  //   LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
  //   return RC::SCHEMA_TABLE_NOT_EXIST;
  // }

  // check the field number


  // stmt = new UpdateStmt(table, )
  return RC::INTERNAL;
}
