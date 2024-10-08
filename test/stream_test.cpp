// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

#include "test_utils.hpp"
#include <aws/store/filesystem/posixFileSystem.hpp>
#include <aws/store/stream/fileStream.hpp>
#include <catch2/catch_test_macros.hpp>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

class StreamLogger final : public aws::store::logging::Logger {
    void log(const aws::store::logging::LogLevel level, const std::string &msg) const override {
        switch (level) {
        case aws::store::logging::LogLevel::Disabled:
            break;

        case aws::store::logging::LogLevel::Trace:
            break;
        case aws::store::logging::LogLevel::Debug:
            break;
        case aws::store::logging::LogLevel::Info: {
            INFO(msg);
            break;
        }
        case aws::store::logging::LogLevel::Warning: {
            WARN(msg);
            break;
        }
        case aws::store::logging::LogLevel::Error: {
            WARN(msg);
            break;
        }
        }
    }
};

static const auto stream_logger = std::make_shared<StreamLogger>();

static auto open_stream(const std::shared_ptr<aws::store::filesystem::FileSystemInterface> &fs) {
    return aws::store::stream::FileStream::openOrCreate(aws::store::stream::StreamOptions{
        1024 * 1024,
        10 * 1024 * 1024,
        true,
        fs,
        stream_logger,
        aws::store::kv::KVOptions{
            true,
            fs,
            stream_logger,
            "m",
            1 * 1024,
        },
    });
}

static auto read_stream_values_by_segment(std::shared_ptr<aws::store::test::utils::SpyFileSystem> fs,
                                          std::filesystem::path temp_dir_path, uint32_t value_size) {
    auto files_or = fs->list();
    REQUIRE(files_or.ok());
    auto files = std::move(files_or.val());
    std::sort(files.begin(), files.end());
    std::map<std::string, std::vector<std::string>> segments;
    for (const auto &f : files) {
        if (f.rfind(".log") == std::string::npos) {
            continue;
        }
        auto file_or = fs->open(temp_dir_path / f);
        REQUIRE(file_or.ok());
        std::vector<std::string> values;
        auto pos = 0U;
        while (true) {
            pos += aws::store::stream::LOG_ENTRY_HEADER_SIZE;
            auto val_or = file_or.val()->read(pos, pos + value_size);
            if (!val_or.ok()) {
                REQUIRE(val_or.err().code == aws::store::filesystem::FileErrorCode::EndOfFile);
                break;
            }
            values.emplace_back(val_or.val().string());
            pos += value_size;
        }
        segments[f] = std::move(values);
    }
    return segments;
}

SCENARIO("Posix filesystem errors", "[fs]") {
    auto fs = std::make_shared<aws::store::filesystem::PosixFileSystem>("/abc");
    auto file_or = fs->open("def");
    REQUIRE_FALSE(file_or.ok());
    REQUIRE(file_or.err().code == aws::store::filesystem::FileErrorCode::AccessDenied);
}

SCENARIO("I cannot create a stream", "[stream]") {
    auto temp_dir = aws::store::test::utils::TempDir();
    auto fs = std::make_shared<aws::store::test::utils::SpyFileSystem>(
        std::make_shared<aws::store::filesystem::PosixFileSystem>(temp_dir.path()));

    fs->when("open", aws::store::test::utils::SpyFileSystem::OpenType{[](const std::string &) {
                 return aws::store::filesystem::FileError{aws::store::filesystem::FileErrorCode::AccessDenied, {}};
             }})
        ->when("list", aws::store::test::utils::SpyFileSystem::ListType{
                           []() { return std::vector<std::string>{"a.log", "b.log", "1.log", "2.log"}; }});

    auto stream_or = open_stream(fs);
    REQUIRE(!stream_or.ok());
    REQUIRE(stream_or.err().code == aws::store::stream::StreamErrorCode::ReadError);

    stream_or = open_stream(fs);
    REQUIRE(stream_or.ok());

    auto stream = std::move(stream_or.val());
    REQUIRE(stream->firstSequenceNumber() == 1);
    REQUIRE(stream->highestSequenceNumber() == 2);
}

SCENARIO("I create a stream with file failures", "[stream]") {
    auto temp_dir = aws::store::test::utils::TempDir();
    auto fs = std::make_shared<aws::store::test::utils::SpyFileSystem>(
        std::make_shared<aws::store::filesystem::PosixFileSystem>(temp_dir.path()));

    fs->when("open", aws::store::test::utils::SpyFileSystem::OpenType{[&fs](const std::string &s) {
                 auto fakeFile =
                     std::make_unique<aws::store::test::utils::SpyFileLike>(std::move(fs->real->open(s).val()));
                 fakeFile->when("read", aws::store::test::utils::SpyFileLike::ReadType{[](uint32_t, uint32_t) {
                                    return aws::store::filesystem::FileError{
                                        aws::store::filesystem::FileErrorCode::Unknown, {}};
                                }});
                 std::unique_ptr<aws::store::filesystem::FileLike> fileLike{fakeFile.release()};
                 return fileLike;
             }})
        ->when("list",
               aws::store::test::utils::SpyFileSystem::ListType{[]() { return std::vector<std::string>{"1.log"}; }});

    auto stream_or = open_stream(fs);
    REQUIRE(stream_or.ok());

    auto stream = std::move(stream_or.val());
    REQUIRE(stream->firstSequenceNumber() == 1);
    REQUIRE(stream->highestSequenceNumber() == 1);
}

SCENARIO("Stream validates data length", "[stream]") {
    auto temp_dir = aws::store::test::utils::TempDir();
    auto fs = std::make_shared<aws::store::test::utils::SpyFileSystem>(
        std::make_shared<aws::store::filesystem::PosixFileSystem>(temp_dir.path()));
    auto stream_or = open_stream(fs);
    REQUIRE(stream_or.ok());
    auto stream = std::move(stream_or.val());

    std::string value{};
    auto seq_or = stream->append(aws::store::common::BorrowedSlice{value.data(), UINT32_MAX},
                                 aws::store::stream::AppendOptions{});
    REQUIRE(!seq_or.ok());
    REQUIRE(seq_or.err().code == aws::store::stream::StreamErrorCode::RecordTooLarge);
}

SCENARIO("I can append data to a stream", "[stream]") {
    WHEN("Eviction of oldest data is opted for") {
        auto append_opts = aws::store::stream::AppendOptions{{}, true};

        THEN("Stream deletes oldest data when appending to a full stream") {
            auto temp_dir = aws::store::test::utils::TempDir();
            auto fs = std::make_shared<aws::store::test::utils::SpyFileSystem>(
                std::make_shared<aws::store::filesystem::PosixFileSystem>(temp_dir.path()));
            auto stream_or = open_stream(fs);
            REQUIRE(stream_or.ok());
            auto stream = std::move(stream_or.val());

            aws::store::common::OwnedSlice data{1 * 1024 * 1024};

            for (int i = 0; i < 30; i++) {
                auto seq_or = stream->append(aws::store::common::BorrowedSlice{data.data(), data.size()}, append_opts);
                REQUIRE(seq_or.ok());
            }

            // Check that it rolled over by look at the first sequence number and the total size now.
            REQUIRE(stream->firstSequenceNumber() > 0);
            // Stream can fit 9 records since each record is 1MB and there is 32B overhead per record
            REQUIRE(stream->highestSequenceNumber() - stream->firstSequenceNumber() + 1 == 9);
            REQUIRE(stream->currentSizeBytes() == 9 * (1024 * 1024 + aws::store::stream::LOG_ENTRY_HEADER_SIZE));
        }

        THEN("The max stream size is never exceeded") {
            auto temp_dir = aws::store::test::utils::TempDir();
            auto fs = std::make_shared<aws::store::test::utils::SpyFileSystem>(
                std::make_shared<aws::store::filesystem::PosixFileSystem>(temp_dir.path()));
            auto stream_or = aws::store::stream::FileStream::openOrCreate(aws::store::stream::StreamOptions{
                1024,
                5000,
                true,
                fs,
                stream_logger,
                aws::store::kv::KVOptions{
                    true,
                    fs,
                    stream_logger,
                    "m",
                    1 * 1024,
                },
            });
            REQUIRE(stream_or.ok());
            auto stream = std::move(stream_or.val());

            aws::store::common::OwnedSlice data{61};

            for (int i = 0; i < 1000; i++) {
                auto seq_or = stream->append(aws::store::common::BorrowedSlice{data.data(), data.size()}, append_opts);
                REQUIRE(seq_or.ok());
                // We never exceed the max size of the stream
                REQUIRE(stream->currentSizeBytes() <= 5000);
            }
        }
    }
    WHEN("Eviction of oldest data is not opted for") {
        auto append_opts = aws::store::stream::AppendOptions{{}, false};

        THEN("Stream fails when appending to a full stream") {
            auto temp_dir = aws::store::test::utils::TempDir();
            auto fs = std::make_shared<aws::store::test::utils::SpyFileSystem>(
                std::make_shared<aws::store::filesystem::PosixFileSystem>(temp_dir.path()));
            auto stream_or = open_stream(fs);
            REQUIRE(stream_or.ok());
            auto stream = std::move(stream_or.val());

            aws::store::common::OwnedSlice data{1 * 1024 * 1024};

            // fill the stream with 9 records since each record is 1MB and there is 32B overhead per record
            for (int i = 0; i < 9; i++) {
                auto seq_or = stream->append(aws::store::common::BorrowedSlice{data.data(), data.size()}, append_opts);
                REQUIRE(seq_or.ok());
            }
            // Check that it has filled by looking at the total size now
            REQUIRE(stream->highestSequenceNumber() - stream->firstSequenceNumber() + 1 == 9);
            REQUIRE(stream->currentSizeBytes() < 10 * 1024 * 1024);

            // fail when trying to add another record
            auto seq_or = stream->append(aws::store::common::BorrowedSlice{data.data(), data.size()}, append_opts);
            REQUIRE(!seq_or.ok());
            REQUIRE(seq_or.err().code == aws::store::stream::StreamErrorCode::StreamFull);
            // Check that it hasn't rolled over by looking at the first sequence number
            REQUIRE(stream->firstSequenceNumber() == 0);
        }
    }
}

SCENARIO("I can delete an iterator") {
    WHEN("I create an iterator") {
        auto temp_dir = aws::store::test::utils::TempDir();
        auto fs = std::make_shared<aws::store::test::utils::SpyFileSystem>(
            std::make_shared<aws::store::filesystem::PosixFileSystem>(temp_dir.path()));
        auto stream_or = open_stream(fs);
        REQUIRE(stream_or.ok());
        auto stream = std::move(stream_or.val());
        auto app_or = stream->append(aws::store::common::BorrowedSlice{"val"}, aws::store::stream::AppendOptions{});
        REQUIRE(app_or.ok());

        auto it = stream->openOrCreateIterator("ita", aws::store::stream::IteratorOptions{});

        THEN("I checkpoint the iterator") {
            REQUIRE((*it).val().checkpoint().ok());

            THEN("I can delete the iterator") {
                REQUIRE(stream->deleteIterator("ita").ok());
            }
        }

        THEN("I can checkpoint a record returned from an out of scope iterator") {
            aws::store::stream::CheckpointableOwnedRecord record;
            {
                auto it2 = stream->openOrCreateIterator("ita2", aws::store::stream::IteratorOptions{});
                record = std::move((*it2).val());
            }
            REQUIRE(record.checkpoint().ok());
        }

        THEN("I can delete the iterator") {
            REQUIRE(stream->deleteIterator("ita").ok());
        }
    }
}

SCENARIO("I can create a stream", "[stream]") {
    auto temp_dir = aws::store::test::utils::TempDir();
    auto fs = std::make_shared<aws::store::test::utils::SpyFileSystem>(
        std::make_shared<aws::store::filesystem::PosixFileSystem>(temp_dir.path()));
    auto stream_or = open_stream(fs);
    REQUIRE(stream_or.ok());
    auto stream = std::move(stream_or.val());

    const std::string &value =
        GENERATE(take(5, aws::store::test::utils::random(1, 1 * 1024 * 1024, 0, static_cast<char>(255))));

    WHEN("I append values") {
        auto seq_or = stream->append(aws::store::common::BorrowedSlice{value}, aws::store::stream::AppendOptions{});
        REQUIRE(seq_or.ok());
        seq_or = stream->append(aws::store::common::BorrowedSlice{value}, aws::store::stream::AppendOptions{});
        REQUIRE(seq_or.ok());
        seq_or = stream->append(aws::store::common::BorrowedSlice{value}, aws::store::stream::AppendOptions{});
        REQUIRE(seq_or.ok());

        auto it = stream->openOrCreateIterator("ita", aws::store::stream::IteratorOptions{});
        REQUIRE(it.sequence_number == 0);
        auto v_or = *it;
        REQUIRE(v_or.ok());
        REQUIRE(std::string_view{v_or.val().data.char_data(), v_or.val().data.size()} == value);
        REQUIRE(v_or.val().checkpoint().ok());

        ++it;
        REQUIRE(it.sequence_number == 1);
        v_or = *it;
        REQUIRE(v_or.ok());
        REQUIRE(v_or.val().checkpoint().ok());

        // New iterator starts at 0
        auto other_it = stream->openOrCreateIterator("other", aws::store::stream::IteratorOptions{});
        REQUIRE(other_it.sequence_number == 0);
        // Reopening the existing iterator does not move it forward
        other_it = stream->openOrCreateIterator("other", aws::store::stream::IteratorOptions{});
        REQUIRE(other_it.sequence_number == 0);

        it = stream->openOrCreateIterator("ita", aws::store::stream::IteratorOptions{});
        // Pointing at the first unread record which is 2 because we checkpointed after we read 1.
        REQUIRE(it.sequence_number == 2);

        // Close and reopen stream from FS. Verify that our iterator is where we left it.
        stream.reset();
        stream_or = open_stream(fs);
        REQUIRE(stream_or.ok());
        stream = std::move(stream_or.val());

        it = stream->openOrCreateIterator("ita", aws::store::stream::IteratorOptions{});
        REQUIRE(it.sequence_number == 2);
        other_it = stream->openOrCreateIterator("other", aws::store::stream::IteratorOptions{});
        REQUIRE(other_it.sequence_number == 0);

        ++it;
        REQUIRE(it.sequence_number == 3);
        v_or = *it;
        REQUIRE(!v_or.ok());
        REQUIRE(v_or.err().code == aws::store::stream::StreamErrorCode::RecordNotFound);

        REQUIRE(stream->deleteIterator("ita").ok());

        AND_WHEN("I close and reopen the stream") {
            stream.reset();

            stream_or = open_stream(fs);
            REQUIRE(stream_or.ok());
            stream = std::move(stream_or.val());

            it = stream->openOrCreateIterator("ita", aws::store::stream::IteratorOptions{});
            REQUIRE(it.sequence_number == 0);
            v_or = *it;
            REQUIRE(v_or.ok());
            REQUIRE(std::string_view{v_or.val().data.char_data(), v_or.val().data.size()} == value);
        }
    }
}

SCENARIO("Stream detects and recovers from corruption", "[stream]") {
    auto temp_dir = aws::store::test::utils::TempDir();
    auto fs = std::make_shared<aws::store::test::utils::SpyFileSystem>(
        std::make_shared<aws::store::filesystem::PosixFileSystem>(temp_dir.path()));

    constexpr auto num_stream_values = 10;
    constexpr auto stream_value_size = 1024 * 1024 / 4;

    auto stream_or = open_stream(fs);
    REQUIRE(stream_or.ok());

    // create stream and populate with random values
    auto stream = std::move(stream_or.val());
    for (auto i = 0; i < num_stream_values; i++) {
        std::string value;
        aws::store::test::utils::random_string(value, stream_value_size);
        REQUIRE(stream->append(aws::store::common::BorrowedSlice{value}, aws::store::stream::AppendOptions{}).ok());
    }

    // sanity check: verify we can read all values in the stream
    for (auto i = 0U; i < num_stream_values; i++) {
        REQUIRE(stream->read(i, aws::store::stream::ReadOptions{}).ok());
    }

    std::map<std::string, std::vector<std::string>> segments =
        read_stream_values_by_segment(fs, temp_dir.path(), stream_value_size);
    REQUIRE(segments.size() > 1);

    auto first_segment = segments.begin();
    auto second_segment = std::next(segments.begin(), 1);

    WHEN("I corrupt the second header in the first segment") {
        std::fstream file(temp_dir.path() / first_segment->first, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(file);
        file.seekp(aws::store::stream::LOG_ENTRY_HEADER_SIZE + stream_value_size);
        std::string corrupted_header = "A";
        file.write(corrupted_header.c_str(), static_cast<std::streamsize>(corrupted_header.size()));
        file.close();

        THEN("Reading data in segment, starting at corrupted entry, fails") {

            // first entry isn't corrupted
            auto val_or = stream->read(0, aws::store::stream::ReadOptions{});
            REQUIRE(val_or.ok());
            REQUIRE(val_or.val().data.string() == first_segment->second[0]);

            // rest of the current segment is unreadable
            for (auto i = 1U; i < first_segment->second.size(); i++) {
                auto data_or = stream->read(i, aws::store::stream::ReadOptions{});
                REQUIRE(!data_or.ok());
                REQUIRE(data_or.err().code == aws::store::stream::StreamErrorCode::RecordNotFound);
            }

            // reading data in next segment still works
            val_or = stream->read(first_segment->second.size(), aws::store::stream::ReadOptions{});
            REQUIRE(val_or.ok());
            REQUIRE(val_or.val().data.string() == second_segment->second[0]);

            THEN("Reading data with may_return_later_records option will skip to the next segment") {
                // first entry isn't corrupted
                val_or = stream->read(0U, aws::store::stream::ReadOptions{{}, true, {}});
                REQUIRE(val_or.ok());
                REQUIRE(val_or.val().data.string() == first_segment->second[0]);

                // instead of reading next entry (which is corrupted), skip to next segment
                val_or = stream->read(1U, aws::store::stream::ReadOptions{{}, true, {}});
                REQUIRE(val_or.ok());
                REQUIRE(val_or.val().data.string() == second_segment->second[0]);

                AND_WHEN("I reopen the stream") {
                    stream_or = open_stream(fs);
                    REQUIRE(stream_or.ok());
                    stream = std::move(stream_or.val());

                    THEN("Reading data in segment, starting at corrupted entry, fails") {
                        // first entry isn't corrupted
                        val_or = stream->read(0U, aws::store::stream::ReadOptions{});
                        REQUIRE(val_or.ok());
                        REQUIRE(val_or.val().data.string() == first_segment->second[0]);

                        // rest of the current segment is unreadable
                        for (auto i = 1U; i < first_segment->second.size(); i++) {
                            auto data_or = stream->read(i, aws::store::stream::ReadOptions{});
                            REQUIRE(!data_or.ok());
                            REQUIRE(data_or.err().code == aws::store::stream::StreamErrorCode::RecordNotFound);
                        }

                        // reading data in next segment still works
                        val_or = stream->read(first_segment->second.size(), aws::store::stream::ReadOptions{});
                        REQUIRE(val_or.ok());
                        REQUIRE(val_or.val().data.string() == second_segment->second[0]);

                        THEN("Reading data with may_return_later_records option will skip to the next segment") {
                            // first entry isn't corrupted
                            val_or = stream->read(0U, aws::store::stream::ReadOptions{{}, true, {}});
                            REQUIRE(val_or.ok());
                            REQUIRE(val_or.val().data.string() == first_segment->second[0]);

                            // instead of reading next entry (which is corrupted), skip to next segment
                            val_or = stream->read(1U, aws::store::stream::ReadOptions{{}, true, {}});
                            REQUIRE(val_or.ok());
                            REQUIRE(val_or.val().data.string() == second_segment->second[0]);
                        }
                    }
                }
            }
        }
    }
}

SCENARIO("Old records can be removed", "[stream]") {
    auto temp_dir = aws::store::test::utils::TempDir();
    auto fs = std::make_shared<aws::store::test::utils::SpyFileSystem>(
        std::make_shared<aws::store::filesystem::PosixFileSystem>(temp_dir.path()));

    auto stream_or = open_stream(fs);
    REQUIRE(stream_or.ok());
    auto stream = std::move(stream_or.val());

    WHEN("I append enough values to create multiple stream segments") {
        std::string value;
        uint32_t stream_value_size = 1024 * 1024 / 4; // segment size is 1024 * 1024
        aws::store::test::utils::random_string(value, stream_value_size);
        uint64_t num_records = 20; // put in enough records to create multiple segments

        for (uint64_t i = 0; i < num_records; ++i) {
            auto seq_or = stream->append(aws::store::common::BorrowedSlice{value}, aws::store::stream::AppendOptions{});
            REQUIRE(seq_or.ok());
            std::this_thread::sleep_for(std::chrono::milliseconds(5)); // give each record a distinct timestamp
        }

        // verify stream has multiple segments
        std::map<std::string, std::vector<std::string>> segments =
            read_stream_values_by_segment(fs, temp_dir.path(), stream_value_size);
        REQUIRE(segments.size() > 3);

        // reopen the stream to make sure we can read
        // latest timestamp properly from segment headers
        stream_or = open_stream(fs);
        REQUIRE(stream_or.ok());
        stream = std::move(stream_or.val());

        THEN("I can expire records from a segment") {
            auto iter = segments.begin();
            auto first_seq_num_second_seg = iter->second.size();

            // lookup a record within the second segment
            auto v_or = stream->read(first_seq_num_second_seg + 1, aws::store::stream::ReadOptions{});
            REQUIRE(v_or.ok());

            auto size_before = stream->currentSizeBytes();
            stream->removeOlderRecords(v_or.val().timestamp);
            REQUIRE(size_before > stream->currentSizeBytes());

            // only the first segment is removed.
            // second segment is NOT removed, because not ALL of its records are expired
            for (uint64_t i = 0; i < num_records; ++i) {
                v_or = stream->read(i, aws::store::stream::ReadOptions{});
                auto expected = i >= first_seq_num_second_seg;
                REQUIRE(v_or.ok() == expected);
            }
        }

        THEN("I can remove old records") {
            auto curr_timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                         std::chrono::system_clock::now().time_since_epoch())
                                         .count();
            auto size_before = stream->currentSizeBytes();
            stream->removeOlderRecords(curr_timestamp_ms + 5000);
            REQUIRE(size_before > stream->currentSizeBytes());

            // all values have been removed
            for (uint64_t i = 0; i < num_records; ++i) {
                auto v_or = stream->read(i, aws::store::stream::ReadOptions{});
                REQUIRE(!v_or.ok());
            }

            THEN("I can still append new records") {
                for (uint64_t i = 0; i < num_records; ++i) {
                    auto seq_or =
                        stream->append(aws::store::common::BorrowedSlice{"val"}, aws::store::stream::AppendOptions{});
                    REQUIRE(seq_or.ok());
                }
                for (uint64_t i = num_records; i < num_records * 2; ++i) {
                    auto v_or = stream->read(i, aws::store::stream::ReadOptions{});
                    REQUIRE(v_or.ok());
                }
            }
        }
        THEN("I can't remove later records") {
            auto v_or = stream->read(0, aws::store::stream::ReadOptions{});
            REQUIRE(v_or.ok());

            auto size_before = stream->currentSizeBytes();
            stream->removeOlderRecords(v_or.val().timestamp);
            REQUIRE(size_before == stream->currentSizeBytes());

            // no values have been removed
            for (uint64_t i = 0; i < num_records; ++i) {
                v_or = stream->read(i, aws::store::stream::ReadOptions{});
                REQUIRE(v_or.ok());
            }
        }
    }
}
