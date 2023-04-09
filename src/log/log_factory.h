#ifndef DBTRAIN_LOG_FACTORY_H
#define DBTRAIN_LOG_FACTORY_H

#include "../defines.h"
#include "log.h"
#include "log/log_image.h"

namespace dbtrain {

class LogFactory {
 public:
  struct TxInfo {
    LSN lsn;
    LSN prev_lsn;
    XID xid;
  };
  static Log *LoadLog(const Byte *src);
  static size_t StoreLog(Byte *dst, Log *log);

  static Log *NewInsertLog(const TxInfo &info, const string &table_name, Rid rid, size_t new_len, const void *new_val);
  static Log *NewDeleteLog(const TxInfo &info, const string &table_name, Rid rid, size_t old_len, const void *old_val);
  static Log *NewUpdateLog(const TxInfo &info, const string &table_name, Rid rid, size_t old_len, const void *old_val,
                           size_t new_len, const void *new_val);
  static Log *NewCLRLog(const TxInfo &info, LSN undo_next_lsn, PhysiologicalImage& log_image);
};

}  // namespace dbtrain

#endif
