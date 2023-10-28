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
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/date.h"

UpdateStmt::UpdateStmt(Table *table, const Value *values, int value_amount, FilterStmt *filter_stmt)
    : table_(table), values_(values), value_amount_(value_amount), filter_stmt_(filter_stmt)
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
  // check the fields type and fields name
  bool             flag          = false;
  const Value     *update_values        = &update.value;
  Value * values = const_cast<Value *>(update_values);
  const TableMeta &table_meta    = table->table_meta();
  const int        sys_field_num = table_meta.sys_field_num();
  const AttrType   value_type    = values[0].attr_type();
  for (int i = 0; i < table_meta.field_num()-sys_field_num; i++) {
    const FieldMeta *field_meta = table_meta.field(i + sys_field_num);
    const std::string   field_name = field_meta->name();
    const AttrType      attr_type  = field_meta->type();
    if (field_name == update.attribute_name) {
      flag = true;
      if (values[0].attr_type()==AttrType::NULLS){
        if (!field_meta->nullable()){
          return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        }
        values[0].set_null(1 << i);
      }
      if (attr_type == AttrType::DATES && value_type == AttrType::CHARS) {
        int date = -1;
        RC rc = string_to_date(values[0].data(), date);
        if (rc != RC::SUCCESS) {
          if (rc == RC::INVALID_ARGUMENT) {
            LOG_WARN("Can not parse date. The format must be YYYY-MM-DD, and the date must be valid. table=%s, field=%s, value=%s",
              table_name, field_meta->name(), values[0].data());
          }
          return rc;
        }
        values[0].set_date(date);
      }
      if (values[0].attr_type()!=AttrType::NULLS && attr_type!=values[0].attr_type()){
        LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
        table_name, field_meta->name(), attr_type, value_type);
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
      update_fields.push_back(Field(table, field_meta));
      break;
    }
  }
  if (flag == false) {
    LOG_WARN("field name not found. table=%s, value name=%d", table_name, update.attribute_name);
    return RC::INVALID_ARGUMENT;
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
  UpdateStmt *update_stmt = new UpdateStmt(table, values, 1, filter_stmt);
  update_stmt->update_fields_.swap(update_fields);
  stmt = update_stmt;
  return rc;
}
