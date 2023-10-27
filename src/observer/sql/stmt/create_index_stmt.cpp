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
// Created by Wangyunlai on 2023/4/25.
//

#include "sql/stmt/create_index_stmt.h"
#include "storage/table/table.h"
#include "storage/db/db.h"
#include "common/lang/string.h"
#include "common/log/log.h"

using namespace std;
using namespace common;

RC CreateIndexStmt::create(Db *db, const CreateIndexSqlNode &create_index, Stmt *&stmt)
{
  stmt = nullptr;

  const char *table_name = create_index.relation_name.c_str();
  if (is_blank(table_name) || is_blank(create_index.index_name.c_str()) || create_index.attributes.size() == 0) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, index name=%s",
        db, table_name, create_index.index_name.c_str());
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  std::vector<FieldMeta> fields;
  for (string attribute_name: create_index.attributes){
    const FieldMeta *field_meta = table->table_meta().field(attribute_name.c_str());
    if (nullptr == field_meta) {
      LOG_WARN("no such field in table. db=%s, table=%s, field name=%s", 
              db->name(), table_name, attribute_name);
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }
    fields.emplace_back(*field_meta);
  }
  

  Index *index = table->find_index(create_index.index_name.c_str());
  if (nullptr != index) {
    LOG_WARN("index with name(%s) already exists. table name=%s", create_index.index_name.c_str(), table_name);
    return RC::SCHEMA_INDEX_NAME_REPEAT;
  }

  stmt = new CreateIndexStmt(table, fields, create_index.index_name);
  return RC::SUCCESS;
}
