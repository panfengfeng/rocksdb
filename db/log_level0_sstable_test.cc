//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/write_batch.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"

#include "db/log_reader.h"
#include "db/write_batch_internal.h"
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "util/filename.h"
#include "util/file_reader_writer.h"

#include <iostream>

namespace rocksdb {
    Status loglevel0sstable() {
   // Env
        Env* env = Env::Default();
   // writebatch
        WriteBatch batch;
        unique_ptr<WritableFile> lfile;
        log::Writer* new_log = nullptr;
        std::string fname = LogFileName("./", 123456);
        Status s;
        uint64_t size[2];
        uint64_t totalsize = 0;
        s = NewWritableFile(env, fname,
                                &lfile, EnvOptions());

        unique_ptr<WritableFileWriter> file_writer(
                    new WritableFileWriter(std::move(lfile), EnvOptions()));
        new_log = new log::Writer(std::move(file_writer), 123456,
                                      false, false);
        for (int i = 0; i < 2; i++) {
            if (i == 0) {
                batch.Put("panfengfeng", "gaolulu");
            } else {
                batch.Put("zhaojicheng", "zouyanyan");
                batch.Put("wahaha", "huluwa");
                batch.Put("bannilu", "senma");
            }
            std::cout << "batch kv count " << batch.Count() << std::endl;

            s = env->GetFileSize(fname, &size[i]);
            std::cout << "offset " << size[i] << std::endl;

            WriteBatchInternal::SetSequence(&batch, i);
            Slice log_entry = WriteBatchInternal::Contents(&batch);

            s = new_log->AddRecord(log_entry);

            if (s.ok()) {
                std::cout << "addrecord right!" << std::endl;
            } else {
                std::cout << "addrecord wrong!" << std::endl;
            }
            batch.Clear();
        }

        delete new_log;

        s = env->GetFileSize(fname, &totalsize);
        std::cout << "file size " << totalsize << std::endl;

     // readrecord
        unique_ptr<SequentialFileReader> file_reader;
        {
                unique_ptr<SequentialFile> file;
                s = env->NewSequentialFile(fname, &file,
                                                 env->OptimizeForLogRead(EnvOptions()));
                if (!s.ok()) {
                    std::cout << "return error!" << std::endl;
                    exit(-1);
                }
                file_reader.reset(new SequentialFileReader(std::move(file)));
        }

        log::Reader reader(nullptr, std::move(file_reader),
                           nullptr, true /*checksum*/, size[0] /*initial_offset*/,
                           123456);

        std::string scratch;
        Slice record;
        WriteBatch readbatch;
        int i = 1;
        while (reader.ReadRecord(&record, &scratch,
                                 WALRecoveryMode::kPointInTimeRecovery) && s.ok()) {
            WriteBatchInternal::SetContents(&readbatch, record);
            SequenceNumber sequence = WriteBatchInternal::Sequence(&readbatch);
            std::cout << i << "'s sequence " << sequence << std::endl;
            std::cout << i << "'s record size " << record.size() << std::endl;
            std::cout << i << "'s batch size " << readbatch.GetDataSize() << std::endl;

            Slice input(readbatch.Data());
            input.remove_prefix(WriteBatchInternal::kHeader);
            Slice key, value, blob, xid;
            int found = 0;
            int count = readbatch.Count();

            while (s.ok() && !input.empty()) {
                char tag = 0;
                uint32_t column_family = 0;  // default

                s = ReadRecordFromWriteBatch(&input, &tag, &column_family, &key, &value,
                                             &blob, &xid);

                if (!s.ok()) {
                    return s;
                }

                switch (tag) {
                    case kTypeColumnFamilyValue:
                    case kTypeValue:
                        found++;
                        std::cout << found << " kv pair " << key.ToString() << " " << value.ToString() << std::endl;
                        break;
                    case kTypeColumnFamilyDeletion:
                    case kTypeDeletion:
                        found++;
                        break;
                    case kTypeColumnFamilySingleDeletion:
                    case kTypeSingleDeletion:
                        found++;
                        break;
                    case kTypeColumnFamilyRangeDeletion:
                    case kTypeRangeDeletion:
                        found++;
                        break;
                    case kTypeColumnFamilyMerge:
                    case kTypeMerge:
                        found++;
                        break;
                    case kTypeLogData:
                        break;
                    case kTypeBeginPrepareXID:
                        break;
                    case kTypeEndPrepareXID:
                        break;
                    case kTypeCommitXID:
                        break;
                    case kTypeRollbackXID:
                        break;
                    case kTypeNoop:
                        break;
                    default:
                        return Status::Corruption("unknown WriteBatch tag");
                }
            }
            i++;
            std::cout << "readbatch count " << count << std::endl;
            std::cout << "found " << found << std::endl;
        }
        return s;
    }
}  // namespace rocksdb

int main(int argc, char** argv) {
    rocksdb::loglevel0sstable();
    return 0;
}
