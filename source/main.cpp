// NOLINTBEGIN
#undef NDEBUG

#include <array>
#include <cassert>
#include <filesystem>
#include <iostream>

#include "fileDb.hpp"
#include "memoryDb.hpp"
#include "posixFileSystem.hpp"
#include <string_view>

#if __APPLE__

#include <mach/mach.h>

void do_memory() {
    kern_return_t ret;
    mach_task_basic_info_data_t info;
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;

    ret = task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &count);
    if (ret != KERN_SUCCESS || count != MACH_TASK_BASIC_INFO_COUNT) {
        fprintf(stderr, "task_info failed: %d\n", ret);
        exit(EXIT_FAILURE);
    }

    printf("resident size max: %llu KB\n", (info.resident_size_max) / 1024);
    printf("resident size now: %llu KB\n", (info.resident_size) / 1024);
}

#else
void do_memory() { printf("Collecting memory info not implemented for this platform\n"); }
#endif

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
        using namespace std::literals;

        // auto s = MemoryStream::openOrCreate(StreamOptions{.maximum_db_size_bytes = 500 * 1024 * 1024});
        auto s = FileStream::openOrCreate(StreamOptions{
            .minimum_segment_size_bytes = 1024 * 1024,
            .maximum_db_size_bytes = 10 * 1024 * 1024,
            .file_implementation = std::make_unique<PosixFileSystem>(std::filesystem::current_path() / "stream1")});

        uint64_t last_sequence_number = 0;
        for (int i = 0; i < NUM_RECORDS; i++) {
            last_sequence_number =
                s->append(BorrowedSlice{reinterpret_cast<const uint8_t *>(data.data()), data.size()});
        }

        try {
            std::cout << s->read(last_sequence_number).data.stringView() << std::endl;
        } catch (const std::exception &e) {
            std::cout << e.what() << std::endl;
        }

        try {
            for (auto r : s->openOrCreateIterator('a', IteratorOptions{})) {
                // Do something with the record....
                // std::cout << r.sequence_number << std::endl;

                r.checkpoint();
            }
        } catch (const std::exception &e) {
            std::cout << e.what() << std::endl;
        }

        std::cout << "last checkpoint: " << s->openOrCreateIterator('a', IteratorOptions{}).sequence_number
                  << std::endl;
        s->deleteIterator('a');
        std::cout << "after deleting iterator: " << s->openOrCreateIterator('a', IteratorOptions{}).sequence_number
                  << std::endl;
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;
    do_memory();
}
// NOLINTEND
