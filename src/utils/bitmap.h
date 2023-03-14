#ifndef DBTRAIN_BITMAP_H
#define DBTRAIN_BITMAP_H

#include <cstdint>

namespace dbtrain {

class Bitmap {
 public:
  Bitmap() = default;
  Bitmap(uint8_t *data, int size);

  void Init();
  void Set(int pos);
  void Reset(int pos);
  bool Test(int pos);

  int FirstFree();
  int NextFree(int base);
  int FirstNotFree();
  int NextNotFree(int base);
  int FirstBit(bool bit);
  int NextBit(int base, bool bit);
  bool Full();
  bool Empty();
  void Display();

 private:
  int GetBucket(int pos);
  uint8_t GetBit(int pos);
  uint8_t *data_;
  int size_;
};

}  // namespace dbtrain

#endif  // DBTRAIN_BITMAP_H
