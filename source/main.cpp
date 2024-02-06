// NOLINTBEGIN
#undef NDEBUG

#include <array>
#include <cassert>
#include <filesystem>
#include <iostream>

#include "fileDb.hpp"
#include "memoryDb.hpp"
#include "posixFileSystem.hpp"
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

    // std::filesystem::remove_all(std::filesystem::current_path() / "stream1");

    auto start = std::chrono::high_resolution_clock::now();
    {
        using namespace aws::gg;

        // auto s = MemoryStream::openOrCreate(StreamOptions{.maximum_db_size_bytes = 500 * 1024 * 1024});
        auto s = FileStream::openOrCreate(StreamOptions{.minimum_segment_size_bytes = 1024 * 1024,
                                                        .maximum_db_size_bytes = 10 * 1024 * 1024,
                                                        .file_implementation = std::make_unique<PosixFileSystem>(
                                                            std::filesystem::current_path() / "stream1")})
                     .val();

        std::cout << "loaded checkpoint: " << s->openOrCreateIterator("a", IteratorOptions{}).sequence_number
                  << std::endl;

        expected<uint64_t, DBError> last_sequence_number{0};
        for (int i = 0; i < NUM_RECORDS; i++) {
            last_sequence_number =
                s->append(BorrowedSlice{reinterpret_cast<const uint8_t *>(data.data()), data.size()});
        }

        auto last_record_or = s->read(last_sequence_number.val());
        if (last_record_or) {
            std::cout << last_record_or.val().data.string() << std::endl;
        } else {
            std::cerr << last_record_or.err().msg << std::endl;
        }

        try {
            for (auto r : s->openOrCreateIterator("a", IteratorOptions{})) {
                // Do something with the record....
                // std::cout << r.sequence_number << std::endl;

                r.checkpoint();
            }
        } catch (const std::exception &e) {
            std::cout << e.what() << std::endl;
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
