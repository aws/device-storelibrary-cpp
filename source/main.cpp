// NOLINTBEGIN
#undef NDEBUG

#include <array>
#include <cassert>
#include <filesystem>
#include <iostream>

#include "filesystem/posixFileSystem.hpp"
#include "stream/fileStream.hpp"
#include "stream/memoryStream.hpp"
#include <sys/resource.h>

void do_memory() {
    struct rusage rusage {};
    getrusage(RUSAGE_SELF, &rusage);

    printf("resident size max: %lu KB\n", (rusage.ru_maxrss) / 1024);
}

int main() {
    srand(time(nullptr));
    auto data = std::array<char, 128>{};
    for (char &i : data) {
        i = (rand() % 64) + 64;
    }

    constexpr int NUM_RECORDS = 100000;

    std::filesystem::remove_all(std::filesystem::current_path() / "stream1");

    auto start = std::chrono::high_resolution_clock::now();
    {
        using namespace aws::gg;
        using namespace aws::gg::kv;

        auto fs = std::make_shared<PosixFileSystem>(std::filesystem::current_path() / "stream1");

        /*
        auto kv_or = KV::openOrCreate(KVOptions{
            .filesystem_implementation = fs,
            .identifier = "m",
            .compact_after = 16 * 1024 * 1024,
        });
        if (!kv_or) {
            std::cerr << kv_or.err().msg << std::endl;
            std::terminate();
        }
        auto kv = kv_or.val();
        for (int i = 0; i < NUM_RECORDS; i++) {
            auto x = kv->put("key" + std::to_string(i), BorrowedSlice{data.data(), data.size()});
        }
     */

        // auto s = MemoryStream::openOrCreate(StreamOptions{.maximum_size_bytes = 500 * 1024 * 1024});
        auto s_or = FileStream::openOrCreate(StreamOptions{
            .minimum_segment_size_bytes = 1024 * 1024,
            .maximum_size_bytes = 10 * 1024 * 1024,
            .file_implementation = fs,
            .kv_options =
                KVOptions{
                    .filesystem_implementation = fs,
                    .identifier = "m",
                    .compact_after = 512 * 1024,
                },
        });
        if (!s_or) {
            std::cerr << s_or.err().msg << std::endl;
            std::terminate();
        }
        auto s = s_or.val();

        std::cout << "loaded checkpoint: " << s->openOrCreateIterator("a", IteratorOptions{}).sequence_number
                  << std::endl;

        expected<uint64_t, StreamError> last_sequence_number{0};
        for (int i = 0; i < NUM_RECORDS; i++) {
            last_sequence_number = s->append(BorrowedSlice{data.data(), data.size()});
        }

        auto last_record_or = s->read(last_sequence_number.val());
        if (last_record_or) {
            std::cout << last_record_or.val().data.string() << std::endl;
        } else {
            std::cerr << last_record_or.err().msg << std::endl;
        }

        for (auto r : s->openOrCreateIterator("a", IteratorOptions{})) {
            // Do something with the record....
            // std::cout << r.sequence_number << std::endl;

            if (r) {
                r.val().checkpoint();
            } else {
                std::cout << r.err().msg << std::endl;
                break;
            }
        }

        std::cout << "last checkpoint: " << s->openOrCreateIterator("a", IteratorOptions{}).sequence_number
                  << std::endl;
        s->deleteIterator("a");
        std::cout << "after deleting iterator: " << s->openOrCreateIterator("a", IteratorOptions{}).sequence_number
                  << std::endl;
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;
    do_memory();
}
// NOLINTEND
