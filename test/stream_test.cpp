#include "filesystem/posixFileSystem.hpp"
#include "stream/fileStream.hpp"
#include "test_utils.hpp"
#include <catch2/catch_test_macros.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

using namespace aws::gg;
using namespace std::string_view_literals;
using namespace std::string_literals;

class Logger : public logging::Logger {
    void log(logging::LogLevel level, const std::string &msg) const override {
        switch (level) {
        case logging::LogLevel::Disabled:
            break;

        case logging::LogLevel::Trace:
            break;
        case logging::LogLevel::Debug:
            break;
        case logging::LogLevel::Info: {
            INFO(msg);
            break;
        }
        case logging::LogLevel::Warning: {
            WARN(msg);
            break;
        }
        case logging::LogLevel::Error: {
            WARN(msg);
            break;
        }
        }
    }
};

static const auto logger = std::make_shared<Logger>();

static auto open_stream(std::shared_ptr<FileSystemInterface> fs) {
    return FileStream::openOrCreate(StreamOptions{
        .minimum_segment_size_bytes = 1024 * 1024,
        .maximum_size_bytes = 10 * 1024 * 1024,
        .full_corruption_check_on_open = true,
        .file_implementation = fs,
        .logger = logger,
        .kv_options =
            kv::KVOptions{
                .full_corruption_check_on_open = true,
                .filesystem_implementation = fs,
                .logger = logger,
                .identifier = "m",
                .compact_after = 1 * 1024,
            },
    });
}

SCENARIO("I cannot create a stream", "[stream]") {
    auto temp_dir = TempDir();
    auto fs = std::make_shared<SpyFileSystem>(std::make_shared<PosixFileSystem>(temp_dir.path()));

    fs->when("open", SpyFileSystem::OpenType{[](__attribute__((unused)) const std::string &s) {
                 return FileError{FileErrorCode::AccessDenied, {}};
             }})
        ->when("list", SpyFileSystem::ListType{[]() {
                   return std::vector<std::string>{"a.log", "b.log", "1.log", "2.log"};
               }});

    auto stream_or = open_stream(fs);
    REQUIRE(!stream_or);
    REQUIRE(stream_or.err().code == StreamErrorCode::ReadError);

    stream_or = open_stream(fs);
    REQUIRE(stream_or);

    auto stream = std::move(stream_or.val());
    REQUIRE(stream->firstSequenceNumber() == 1);
    REQUIRE(stream->highestSequenceNumber() == 2);
}

SCENARIO("Stream validates data length", "[stream]") {
    auto temp_dir = TempDir();
    auto fs = std::make_shared<SpyFileSystem>(std::make_shared<PosixFileSystem>(temp_dir.path()));
    auto stream_or = open_stream(fs);
    REQUIRE(stream_or);
    auto stream = std::move(stream_or.val());

    std::string value{};
    auto seq_or = stream->append(BorrowedSlice{value.data(), UINT32_MAX}, AppendOptions{});
    REQUIRE(!seq_or);
    REQUIRE(seq_or.err().code == StreamErrorCode::RecordTooLarge);
}

SCENARIO("Stream deletes oldest data when full", "[stream]") {
    auto temp_dir = TempDir();
    auto fs = std::make_shared<SpyFileSystem>(std::make_shared<PosixFileSystem>(temp_dir.path()));
    auto stream_or = open_stream(fs);
    REQUIRE(stream_or);
    auto stream = std::move(stream_or.val());

    OwnedSlice data{1 * 1024 * 1024};

    for (int i = 0; i < 30; i++) {
        auto seq_or = stream->append(BorrowedSlice{data.data(), data.size()}, AppendOptions{});
        REQUIRE(seq_or);
    }

    // Check that it rolled over by look at the first sequence number and the total size now.
    REQUIRE(stream->firstSequenceNumber() > 0);
    // Stream can fit 9 records since each record is 1MB and there is 32B overhead per record
    REQUIRE(stream->highestSequenceNumber() - stream->firstSequenceNumber() + 1 == 9);
    REQUIRE(stream->currentSizeBytes() < 10 * 1024 * 1024);
}

SCENARIO("I can create a stream", "[stream]") {
    auto temp_dir = TempDir();
    auto fs = std::make_shared<SpyFileSystem>(std::make_shared<PosixFileSystem>(temp_dir.path()));
    auto stream_or = open_stream(fs);
    REQUIRE(stream_or);
    auto stream = std::move(stream_or.val());

    const std::string &value = GENERATE(take(5, random(1, 1 * 1024 * 1024, 0, static_cast<char>(255))));

    WHEN("I append values") {
        auto seq_or = stream->append(BorrowedSlice{value}, AppendOptions{});
        REQUIRE(seq_or);
        seq_or = stream->append(BorrowedSlice{value}, AppendOptions{});
        REQUIRE(seq_or);
        seq_or = stream->append(BorrowedSlice{value}, AppendOptions{});
        REQUIRE(seq_or);

        auto it = stream->openOrCreateIterator("ita", IteratorOptions{});
        REQUIRE(it.sequence_number == 0);
        auto v_or = *it;
        REQUIRE(v_or);
        REQUIRE(std::string_view{v_or.val().data.char_data(), v_or.val().data.size()} == value);
        v_or.val().checkpoint();

        ++it;
        REQUIRE(it.sequence_number == 1);
        v_or = *it;
        REQUIRE(v_or);
        v_or.val().checkpoint();

        // New iterator starts at 0
        auto other_it = stream->openOrCreateIterator("other", IteratorOptions{});
        REQUIRE(other_it.sequence_number == 0);
        // Reopening the existing iterator does not move it forward
        other_it = stream->openOrCreateIterator("other", IteratorOptions{});
        REQUIRE(other_it.sequence_number == 0);

        it = stream->openOrCreateIterator("ita", IteratorOptions{});
        // Pointing at the first unread record which is 2 because we checkpointed after we read 1.
        REQUIRE(it.sequence_number == 2);

        // Close and reopen stream from FS. Verify that our iterator is where we left it.
        stream.reset();
        stream_or = open_stream(fs);
        REQUIRE(stream_or);
        stream = std::move(stream_or.val());

        it = stream->openOrCreateIterator("ita", IteratorOptions{});
        REQUIRE(it.sequence_number == 2);
        other_it = stream->openOrCreateIterator("other", IteratorOptions{});
        REQUIRE(other_it.sequence_number == 0);

        ++it;
        REQUIRE(it.sequence_number == 3);
        v_or = *it;
        REQUIRE(!v_or);
        REQUIRE(v_or.err().code == StreamErrorCode::RecordNotFound);

        REQUIRE(stream->deleteIterator("ita"));

        AND_WHEN("I close and reopen the stream") {
            stream = nullptr;

            stream_or = open_stream(fs);
            REQUIRE(stream_or);
            stream = std::move(stream_or.val());

            it = stream->openOrCreateIterator("ita", IteratorOptions{});
            REQUIRE(it.sequence_number == 0);
            v_or = *it;
            REQUIRE(v_or);
            REQUIRE(std::string_view{v_or.val().data.char_data(), v_or.val().data.size()} == value);
        }
    }
}