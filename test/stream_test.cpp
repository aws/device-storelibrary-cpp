#include <aws/store/filesystem/posixFileSystem.hpp>
#include <aws/store/stream/fileStream.hpp>
#include "test_utils.hpp"
#include <catch2/catch_test_macros.hpp>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

class Logger final : public aws::gg::logging::Logger {
    void log(const aws::gg::logging::LogLevel level, const std::string &msg) const override {
        switch (level) {
        case aws::gg::logging::LogLevel::Disabled:
            break;

        case aws::gg::logging::LogLevel::Trace:
            break;
        case aws::gg::logging::LogLevel::Debug:
            break;
        case aws::gg::logging::LogLevel::Info: {
            INFO(msg);
            break;
        }
        case aws::gg::logging::LogLevel::Warning: {
            WARN(msg);
            break;
        }
        case aws::gg::logging::LogLevel::Error: {
            WARN(msg);
            break;
        }
        }
    }
};

static const auto logger = std::make_shared<Logger>();

static auto open_stream(const std::shared_ptr<aws::gg::FileSystemInterface> &fs) {
    return aws::gg::FileStream::openOrCreate(aws::gg::StreamOptions{
        1024 * 1024,
        10 * 1024 * 1024,
        true,
        fs,
        logger,
        aws::gg::kv::KVOptions{
            true,
            fs,
            logger,
            "m",
            1 * 1024,
        },
    });
}

static constexpr auto log_header_size = 32;

static auto read_stream_values_by_segment(std::shared_ptr<aws::gg::test::utils::SpyFileSystem> fs,
                                          std::filesystem::path temp_dir_path, int value_size) {
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
        auto pos = 0;
        while (true) {
            pos += log_header_size;
            auto val_or = file_or.val()->read(pos, pos + value_size);
            if (!val_or.ok()) {
                REQUIRE(val_or.err().code == aws::gg::FileErrorCode::EndOfFile);
                break;
            }
            values.emplace_back(val_or.val().string());
            pos += value_size;
        }
        segments[f] = std::move(values);
    }
    return segments;
}

SCENARIO("I cannot create a stream", "[stream]") {
    auto temp_dir = aws::gg::test::utils::TempDir();
    auto fs = std::make_shared<aws::gg::test::utils::SpyFileSystem>(
        std::make_shared<aws::gg::PosixFileSystem>(temp_dir.path()));

    fs->when("open", aws::gg::test::utils::SpyFileSystem::OpenType{[](const std::string &) {
                 return aws::gg::FileError{aws::gg::FileErrorCode::AccessDenied, {}};
             }})
        ->when("list", aws::gg::test::utils::SpyFileSystem::ListType{[]() {
                   return std::vector<std::string>{"a.log", "b.log", "1.log", "2.log"};
               }});

    auto stream_or = open_stream(fs);
    REQUIRE(!stream_or.ok());
    REQUIRE(stream_or.err().code == aws::gg::StreamErrorCode::ReadError);

    stream_or = open_stream(fs);
    REQUIRE(stream_or.ok());

    auto stream = std::move(stream_or.val());
    REQUIRE(stream->firstSequenceNumber() == 1);
    REQUIRE(stream->highestSequenceNumber() == 2);
}

SCENARIO("I create a stream with file failures", "[stream]") {
    auto temp_dir = aws::gg::test::utils::TempDir();
    auto fs = std::make_shared<aws::gg::test::utils::SpyFileSystem>(
        std::make_shared<aws::gg::PosixFileSystem>(temp_dir.path()));

    fs->when("open", aws::gg::test::utils::SpyFileSystem::OpenType{[&fs](const std::string &s) {
                 auto fakeFile =
                     std::make_unique<aws::gg::test::utils::SpyFileLike>(std::move(fs->real->open(s).val()));
                 fakeFile->when("read", aws::gg::test::utils::SpyFileLike::ReadType{[](uint32_t, uint32_t) {
                                    return aws::gg::FileError{aws::gg::FileErrorCode::Unknown, {}};
                                }});
                 std::unique_ptr<aws::gg::FileLike> fileLike{fakeFile.release()};
                 return fileLike;
             }})
        ->when("list",
               aws::gg::test::utils::SpyFileSystem::ListType{[]() { return std::vector<std::string>{"1.log"}; }});

    auto stream_or = open_stream(fs);
    REQUIRE(stream_or.ok());

    auto stream = std::move(stream_or.val());
    REQUIRE(stream->firstSequenceNumber() == 1);
    REQUIRE(stream->highestSequenceNumber() == 1);
}

SCENARIO("Stream validates data length", "[stream]") {
    auto temp_dir = aws::gg::test::utils::TempDir();
    auto fs = std::make_shared<aws::gg::test::utils::SpyFileSystem>(
        std::make_shared<aws::gg::PosixFileSystem>(temp_dir.path()));
    auto stream_or = open_stream(fs);
    REQUIRE(stream_or.ok());
    auto stream = std::move(stream_or.val());

    std::string value{};
    auto seq_or = stream->append(aws::gg::BorrowedSlice{value.data(), UINT32_MAX}, aws::gg::AppendOptions{});
    REQUIRE(!seq_or.ok());
    REQUIRE(seq_or.err().code == aws::gg::StreamErrorCode::RecordTooLarge);
}

SCENARIO("I can append data to a stream", "[stream]") {
    WHEN("Eviction of oldest data is opted for") {
        auto append_opts = aws::gg::AppendOptions{{}, true};

        THEN("Stream deletes oldest data when appending to a full stream") {
            auto temp_dir = aws::gg::test::utils::TempDir();
            auto fs = std::make_shared<aws::gg::test::utils::SpyFileSystem>(
                std::make_shared<aws::gg::PosixFileSystem>(temp_dir.path()));
            auto stream_or = open_stream(fs);
            REQUIRE(stream_or.ok());
            auto stream = std::move(stream_or.val());

            aws::gg::OwnedSlice data{1 * 1024 * 1024};

            for (int i = 0; i < 30; i++) {
                auto seq_or = stream->append(aws::gg::BorrowedSlice{data.data(), data.size()}, append_opts);
                REQUIRE(seq_or.ok());
            }

            // Check that it rolled over by look at the first sequence number and the total size now.
            REQUIRE(stream->firstSequenceNumber() > 0);
            // Stream can fit 9 records since each record is 1MB and there is 32B overhead per record
            REQUIRE(stream->highestSequenceNumber() - stream->firstSequenceNumber() + 1 == 9);
            REQUIRE(stream->currentSizeBytes() < 10 * 1024 * 1024);
        }
    }
    WHEN("Eviction of oldest data is not opted for") {
        auto append_opts = aws::gg::AppendOptions{{}, false};

        THEN("Stream fails when appending to a full stream") {
            auto temp_dir = aws::gg::test::utils::TempDir();
            auto fs = std::make_shared<aws::gg::test::utils::SpyFileSystem>(
                std::make_shared<aws::gg::PosixFileSystem>(temp_dir.path()));
            auto stream_or = open_stream(fs);
            REQUIRE(stream_or.ok());
            auto stream = std::move(stream_or.val());

            aws::gg::OwnedSlice data{1 * 1024 * 1024};

            // fill the stream with 9 records since each record is 1MB and there is 32B overhead per record
            for (int i = 0; i < 9; i++) {
                auto seq_or = stream->append(aws::gg::BorrowedSlice{data.data(), data.size()}, append_opts);
                REQUIRE(seq_or.ok());
            }
            // Check that it has filled by looking at the total size now
            REQUIRE(stream->highestSequenceNumber() - stream->firstSequenceNumber() + 1 == 9);
            REQUIRE(stream->currentSizeBytes() < 10 * 1024 * 1024);

            // fail when trying to add another record
            auto seq_or = stream->append(aws::gg::BorrowedSlice{data.data(), data.size()}, append_opts);
            REQUIRE(!seq_or.ok());
            REQUIRE(seq_or.err().code == aws::gg::StreamErrorCode::StreamFull);
            // Check that it hasn't rolled over by looking at the first sequence number
            REQUIRE(stream->firstSequenceNumber() == 0);
        }
    }
}

SCENARIO("I can delete an iterator") {
    WHEN("I create an iterator") {
        auto temp_dir = aws::gg::test::utils::TempDir();
        auto fs = std::make_shared<aws::gg::test::utils::SpyFileSystem>(
            std::make_shared<aws::gg::PosixFileSystem>(temp_dir.path()));
        auto stream_or = open_stream(fs);
        REQUIRE(stream_or.ok());
        auto stream = std::move(stream_or.val());
        auto app_or = stream->append(aws::gg::BorrowedSlice{"val"}, aws::gg::AppendOptions{});
        REQUIRE(app_or.ok());

        auto it = stream->openOrCreateIterator("ita", aws::gg::IteratorOptions{});

        THEN("I checkpoint the iterator") {
            (*it).val().checkpoint();

            THEN("I can delete the iterator") {
                REQUIRE(stream->deleteIterator("ita").ok());
            }
        }

        THEN("I can delete the iterator") {
            REQUIRE(stream->deleteIterator("ita").ok());
        }
    }
}

SCENARIO("I can create a stream", "[stream]") {
    auto temp_dir = aws::gg::test::utils::TempDir();
    auto fs = std::make_shared<aws::gg::test::utils::SpyFileSystem>(
        std::make_shared<aws::gg::PosixFileSystem>(temp_dir.path()));
    auto stream_or = open_stream(fs);
    REQUIRE(stream_or.ok());
    auto stream = std::move(stream_or.val());

    const std::string &value =
        GENERATE(take(5, aws::gg::test::utils::random(1, 1 * 1024 * 1024, 0, static_cast<char>(255))));

    WHEN("I append values") {
        auto seq_or = stream->append(aws::gg::BorrowedSlice{value}, aws::gg::AppendOptions{});
        REQUIRE(seq_or.ok());
        seq_or = stream->append(aws::gg::BorrowedSlice{value}, aws::gg::AppendOptions{});
        REQUIRE(seq_or.ok());
        seq_or = stream->append(aws::gg::BorrowedSlice{value}, aws::gg::AppendOptions{});
        REQUIRE(seq_or.ok());

        auto it = stream->openOrCreateIterator("ita", aws::gg::IteratorOptions{});
        REQUIRE(it.sequence_number == 0);
        auto v_or = *it;
        REQUIRE(v_or.ok());
        REQUIRE(std::string_view{v_or.val().data.char_data(), v_or.val().data.size()} == value);
        v_or.val().checkpoint();

        ++it;
        REQUIRE(it.sequence_number == 1);
        v_or = *it;
        REQUIRE(v_or.ok());
        v_or.val().checkpoint();

        // New iterator starts at 0
        auto other_it = stream->openOrCreateIterator("other", aws::gg::IteratorOptions{});
        REQUIRE(other_it.sequence_number == 0);
        // Reopening the existing iterator does not move it forward
        other_it = stream->openOrCreateIterator("other", aws::gg::IteratorOptions{});
        REQUIRE(other_it.sequence_number == 0);

        it = stream->openOrCreateIterator("ita", aws::gg::IteratorOptions{});
        // Pointing at the first unread record which is 2 because we checkpointed after we read 1.
        REQUIRE(it.sequence_number == 2);

        // Close and reopen stream from FS. Verify that our iterator is where we left it.
        stream.reset();
        stream_or = open_stream(fs);
        REQUIRE(stream_or.ok());
        stream = std::move(stream_or.val());

        it = stream->openOrCreateIterator("ita", aws::gg::IteratorOptions{});
        REQUIRE(it.sequence_number == 2);
        other_it = stream->openOrCreateIterator("other", aws::gg::IteratorOptions{});
        REQUIRE(other_it.sequence_number == 0);

        ++it;
        REQUIRE(it.sequence_number == 3);
        v_or = *it;
        REQUIRE(!v_or.ok());
        REQUIRE(v_or.err().code == aws::gg::StreamErrorCode::RecordNotFound);

        REQUIRE(stream->deleteIterator("ita").ok());

        AND_WHEN("I close and reopen the stream") {
            stream.reset();

            stream_or = open_stream(fs);
            REQUIRE(stream_or.ok());
            stream = std::move(stream_or.val());

            it = stream->openOrCreateIterator("ita", aws::gg::IteratorOptions{});
            REQUIRE(it.sequence_number == 0);
            v_or = *it;
            REQUIRE(v_or.ok());
            REQUIRE(std::string_view{v_or.val().data.char_data(), v_or.val().data.size()} == value);
        }
    }
}

SCENARIO("Stream detects and recovers from corruption", "[stream]") {
    auto temp_dir = aws::gg::test::utils::TempDir();
    auto fs = std::make_shared<aws::gg::test::utils::SpyFileSystem>(
        std::make_shared<aws::gg::PosixFileSystem>(temp_dir.path()));

    constexpr auto num_stream_values = 10;
    constexpr auto stream_value_size = 1024 * 1024 / 4;

    auto stream_or = open_stream(fs);
    REQUIRE(stream_or.ok());

    // create stream and populate with random values
    auto stream = std::move(stream_or.val());
    for (auto i = 0; i < num_stream_values; i++) {
        std::string value;
        aws::gg::test::utils::random_string(value, stream_value_size);
        REQUIRE(stream->append(aws::gg::BorrowedSlice{value}, aws::gg::AppendOptions{}).ok());
    }

    // sanity check: verify we can read all values in the stream
    for (auto i = 0; i < num_stream_values; i++) {
        REQUIRE(stream->read(i, aws::gg::ReadOptions{}).ok());
    }

    std::map<std::string, std::vector<std::string>> segments =
        read_stream_values_by_segment(fs, temp_dir.path(), stream_value_size);
    REQUIRE(segments.size() > 1);

    auto first_segment = segments.begin();
    auto second_segment = std::next(segments.begin(), 1);

    WHEN("I corrupt the second header in the first segment") {
        std::fstream file(temp_dir.path() / first_segment->first, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(file);
        file.seekp(log_header_size + stream_value_size);
        std::string corrupted_header = "A";
        file.write(corrupted_header.c_str(), static_cast<std::streamsize>(corrupted_header.size()));
        file.close();

        THEN("Reading data in segment, starting at corrupted entry, fails") {

            // first entry isn't corrupted
            auto val_or = stream->read(0, aws::gg::ReadOptions{});
            REQUIRE(val_or.ok());
            REQUIRE(val_or.val().data.string() == first_segment->second[0]);

            // rest of the current segment is unreadable
            for (auto i = 1; i < static_cast<int>(first_segment->second.size()); i++) {
                auto data_or = stream->read(i, aws::gg::ReadOptions{});
                REQUIRE(!data_or.ok());
                REQUIRE(data_or.err().code == aws::gg::StreamErrorCode::RecordNotFound);
            }

            // reading data in next segment still works
            val_or = stream->read(first_segment->second.size(), aws::gg::ReadOptions{});
            REQUIRE(val_or.ok());
            REQUIRE(val_or.val().data.string() == second_segment->second[0]);

            THEN("Reading data with may_return_later_records option will skip to the next segment") {
                // first entry isn't corrupted
                val_or = stream->read(0U, aws::gg::ReadOptions{{}, true, {}});
                REQUIRE(val_or.ok());
                REQUIRE(val_or.val().data.string() == first_segment->second[0]);

                // instead of reading next entry (which is corrupted), skip to next segment
                val_or = stream->read(1U, aws::gg::ReadOptions{{}, true, {}});
                REQUIRE(val_or.ok());
                REQUIRE(val_or.val().data.string() == second_segment->second[0]);

                AND_WHEN("I reopen the stream") {
                    stream_or = open_stream(fs);
                    REQUIRE(stream_or.ok());
                    stream = std::move(stream_or.val());

                    THEN("Reading data in segment, starting at corrupted entry, fails") {
                        // first entry isn't corrupted
                        val_or = stream->read(0U, aws::gg::ReadOptions{});
                        REQUIRE(val_or.ok());
                        REQUIRE(val_or.val().data.string() == first_segment->second[0]);

                        // rest of the current segment is unreadable
                        for (auto i = 1; i < static_cast<int>(first_segment->second.size()); i++) {
                            auto data_or = stream->read(i, aws::gg::ReadOptions{});
                            REQUIRE(!data_or.ok());
                            REQUIRE(data_or.err().code == aws::gg::StreamErrorCode::RecordNotFound);
                        }

                        // reading data in next segment still works
                        val_or = stream->read(first_segment->second.size(), aws::gg::ReadOptions{});
                        REQUIRE(val_or.ok());
                        REQUIRE(val_or.val().data.string() == second_segment->second[0]);

                        THEN("Reading data with may_return_later_records option will skip to the next segment") {
                            // first entry isn't corrupted
                            val_or = stream->read(0U, aws::gg::ReadOptions{{}, true, {}});
                            REQUIRE(val_or.ok());
                            REQUIRE(val_or.val().data.string() == first_segment->second[0]);

                            // instead of reading next entry (which is corrupted), skip to next segment
                            val_or = stream->read(1U, aws::gg::ReadOptions{{}, true, {}});
                            REQUIRE(val_or.ok());
                            REQUIRE(val_or.val().data.string() == second_segment->second[0]);
                        }
                    }
                }
            }
        }
    }
}