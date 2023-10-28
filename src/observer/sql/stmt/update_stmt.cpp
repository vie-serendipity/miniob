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

#include "common/log/log.h"
#include "sql/parser/date.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/date.h"

UpdateStmt::UpdateStmt(Table *table, FilterStmt *filter_stmt)
    : table_(table), filter_stmt_(filter_stmt)
{}

UpdateStmt::~UpdateStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  // TODO
  stmt = nullptr;
  const char *table_name = update.relation_name.c_str();
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", db, table_name);
    return RC::INVALID_ARGUMENT;
  }
  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  std::vector<Field> update_fields;
  std::vector<Value> values;
  const TableMeta &table_meta    = table->table_meta();
  const int        sys_field_num = table_meta.sys_field_num();
  // check the fields type and fields name
  for (int i = 0; i < update.set_list.size(); i++) {
    bool             flag          = false;
    values.push_back(update.set_list[i].value);
    for (int j = 0; j < table_meta.field_num() - sys_field_num; j++) {
      const FieldMeta *field_meta = table_meta.field(j + sys_field_num);
      const std::string   field_name = field_meta->name();
      const AttrType      attr_type  = field_meta->type();
      if (field_name == update.set_list[i].attribute_name) {
        flag = true;
        if (values[i].attr_type()==AttrType::NULLS){
          if (!field_meta->nullable()){
            return RC::SCHEMA_FIELD_TYPE_MISMATCH;
          }
          values[i].set_null(1 << i);
        }
        // DATE 类型的字面值是一个字符串，需要在这里对它进行解析和转换
        if (attr_type == AttrType::DATES && values[i].attr_type() == AttrType::CHARS) {
          int date = -1;
          RC rc = string_to_date(values[i].data(), date);
          if (rc != RC::SUCCESS) {
            if (rc == RC::INVALID_ARGUMENT) {
              LOG_WARN("Can not parse date. The format must be YYYY-MM-DD, and the date must be valid. table=%s, field=%s, value=%s",
                table_name, field_meta->name(), values[i].data());
            }
            return rc;
          }
          values[i].set_date(date);
        }
        if (values[i].attr_type()!=AttrType::NULLS && attr_type!=values[i].attr_type()){
          LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
          table_name, field_meta->name(), attr_type, values[i].attr_type());
          return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        }
        update_fields.push_back(Field(table, field_meta));
        break;
      }
    }
    if (flag == false) {
      LOG_WARN("field name not found. table=%s, value name=%d", table_name, update.set_list[i].attribute_name.c_str());
      return RC::INVALID_ARGUMENT;
    }
  }

  // check the conditions
  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));

  FilterStmt *filter_stmt = nullptr;
  RC rc = FilterStmt::create(
      db, table, &table_map, update.conditions.data(), static_cast<int>(update.conditions.size()), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
  }
  UpdateStmt *update_stmt = new UpdateStmt(table, filter_stmt);
  update_stmt->values_.swap(values);
  update_stmt->update_fields_.swap(update_fields);
  stmt = update_stmt;
  return rc;
}
