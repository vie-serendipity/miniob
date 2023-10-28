/* Copyright (c) 2021OceanBase and/or its affiliates. All rights reserved.
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

#include "sql/stmt/insert_stmt.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/date.h"

InsertStmt::InsertStmt(Table *table, std::vector<Value> &values, int value_amount)
    : table_(table), values_(values), value_amount_(value_amount)
{}

RC InsertStmt::create(Db *db, InsertSqlNode &inserts, Stmt *&stmt)
{
  const char *table_name = inserts.relation_name.c_str();
  if (nullptr == db || nullptr == table_name || inserts.values.empty()) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, value_num=%d",
        db, table_name, static_cast<int>(inserts.values.size()));
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // check the fields number
  Value *values = inserts.values.data();
  const int value_num = static_cast<int>(inserts.values.size());
  const TableMeta &table_meta = table->table_meta();
  const int field_num = table_meta.field_num() - table_meta.sys_field_num();
  if (field_num != value_num) {
    LOG_WARN("schema mismatch. value num=%d, field num in schema=%d", value_num, field_num);
    return RC::SCHEMA_FIELD_MISSING;
  }

  // check fields type
  const int sys_field_num = table_meta.sys_field_num();
  int       null_flag     = 0;
  int       length        = 0;
  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field_meta = table_meta.field(i + sys_field_num);
    const AttrType field_type = field_meta->type();
    length += field_meta->len();
    if (values[i].attr_type() == AttrType::NULLS ){
      if (!field_meta->nullable()){
        return RC::INVALID_ARGUMENT;
      }
      switch (field_meta->type())
      {
      case AttrType::INTS:{
        values[i].set_int(0);
      } break;
      case AttrType::FLOATS: {
        values[i].set_float(0);
      } break;
      case AttrType::CHARS: {
        char *data = (char *)malloc(sizeof(char));
        values[i].set_data(data, 1);
      } break;
      case AttrType::DATES: {
        values[i].set_date(0);
      } break;
      case AttrType::UNDEFINED: {
        ASSERT(false, "got an invalid value type");
      } break;
      }
      null_flag |= 1 << i;
    }
    // DATE 类型的字面值是一个字符串，需要在这里对它进行解析和转换
    if (field_type == AttrType::DATES && values[i].attr_type() == AttrType::CHARS) {
      int date = -1;
      RC rc = string_to_date(values[i].data(), date);
      if (rc != RC::SUCCESS) {
        if (rc == RC::INVALID_ARGUMENT) {
          LOG_WARN("Can not parse date. The format must be YYYY-MM-DD, and the date must be valid. table=%s, field=%s, value=%s",
            table_name, field_meta->name(), values[i].data());
        }
        return rc;
      }
      inserts.values[i].set_date(date);
    }
    const AttrType value_type = values[i].attr_type();
    if (value_type != AttrType::NULLS && field_type != value_type) {  // TODO try to convert the value type to field type
      LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
          table_name, field_meta->name(), field_type, value_type);
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  }
  std::vector<Value> res;
  Value null_field;
  null_field.set_null(null_flag);
  res.emplace_back(null_field);
  for (int i = 0; i < value_num; i++) {
    res.emplace_back(values[i]);
  }
  // everything alright
  stmt = new InsertStmt(table, res, value_num + 1);
  return RC::SUCCESS;
}
