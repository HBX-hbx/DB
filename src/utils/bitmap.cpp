#include "bitmap.h"

#include <cstring>
#include <iostream>
#include "../defines.h"

namespace dbtrain {

static const uint8_t BITMAP_HIGHEST_BIT = 1 << (BITMAP_WIDTH - 1);

Bitmap::Bitmap(uint8_t *data, int size) : data_(data), size_(size) {}

void Bitmap::Init() { memset(data_, 0, ((size_ - 1) / BITMAP_WIDTH) + 1); }

void Bitmap::Set(int pos) { std::cerr << "Bitmap::Setting "<< pos << "!\n"; data_[GetBucket(pos)] |= GetBit(pos); }

void Bitmap::Reset(int pos) { std::cerr << "Bitmap::Resetting "<< pos << "!\n"; data_[GetBucket(pos)] &= ~GetBit(pos); }

bool Bitmap::Test(int pos) { return data_[GetBucket(pos)] & GetBit(pos); }

bool Bitmap::Full() { return FirstBit(false) == -1; }

bool Bitmap::Empty() { return FirstBit(true) == -1; }

void Bitmap::Display() {
  std::cerr << "< ------- Display -------- >\n";
  std::cerr << "size: " << size_ << "\n";
  for (size_t i = 0; i < size_; i++) {
    printf("%02X ", data_[i]);
  }
  std::cerr << "\n< ------- Display End -------- >\n";
}

int Bitmap::FirstFree() { return FirstBit(false); }

int Bitmap::FirstNotFree() { return FirstBit(true); }

int Bitmap::NextFree(int base) { return NextBit(base, false); }

int Bitmap::NextNotFree(int base) { return NextBit(base, true); }

int Bitmap::FirstBit(bool bit) { return NextBit(-1, bit); }

int Bitmap::NextBit(int base, bool bit) {
  for (int i = base + 1; i < size_; i++) {
    if (Test(i) == bit) {
      return i;
    }
  }
  return -1;
}

int Bitmap::GetBucket(int pos) { return pos / BITMAP_WIDTH; }

uint8_t Bitmap::GetBit(int pos) { return BITMAP_HIGHEST_BIT >> (pos % BITMAP_WIDTH); }

}  // namespace dbtrain
