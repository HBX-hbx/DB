#ifndef DBTRAIN_RECORD_FACTORY_H
#define DBTRAIN_RECORD_FACTORY_H

#include "../defines.h"
#include "record.h"
#include "../table/table_meta.h"

namespace dbtrain {

class RecordFactory {
 public:
  static Record *ConstInt(int v);
  static Record *ConstFloat(double v);
  static Record *ConstString(string v);

  RecordFactory(const TableMeta *meta);
  ~RecordFactory();

  Record *LoadRecord(const void *src) const;
  void StoreRecord(void *dst, Record *record) const;

  static Field *LoadField(const void *src, FieldType ft, FieldSize fs);
  static void StoreField(void *dst, Field *field, FieldType ft, FieldSize fs);

  // TIPS: 隐藏列操作
  static Rid GetRid(Record *record);
  static void SetRid(Record *record, Rid rid);

  // TODO: 设置MVCC的相关隐藏列
  // LAB 3 BEGIN
  static XID GetCreateXid(Record *record);
  static XID GetDeleteXid(Record *record);
  static void SetCreateXid(Record *record, XID create_xid);
  static void SetDeleteXid(Record *record, XID delete_xid);
  // LAB 3 END

 private:
  const TableMeta *meta_;
};

typedef RecordFactory TupleFactory;

}  // namespace dbtrain

#endif
