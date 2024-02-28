#include "filesystem/posixFileSystem.hpp"
#include "kv/kv.hpp"
#include "test_utils.hpp"
#include <catch2/catch_test_macros.hpp>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

using namespace aws::gg;
using namespace aws::gg::kv;
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

static auto open_kv(const std::string &path) {
    return KV::openOrCreate(KVOptions{
        true,
        std::make_shared<SpyFileSystem>(std::make_shared<PosixFileSystem>(path)),
        logger,
        "test-kv-map",
        0,
    });
}

static auto open_kv_manual_compaction(const std::string &path) {
    return KV::openOrCreate(KVOptions{
        true,
        std::make_shared<SpyFileSystem>(std::make_shared<PosixFileSystem>(path)),
        logger,
        "test-kv-map",
        -1,
    });
}

SCENARIO("I cannot create a KV map with invalid inputs", "[kv]") {
    auto kv_or = KV::openOrCreate(KVOptions{
        {},
        {},
        {},
        "test-kv-map",
        {},
    });
    REQUIRE(!kv_or);
    REQUIRE(kv_or.err().code == KVErrorCodes::InvalidArguments);

    auto temp_dir = TempDir();
    kv_or = KV::openOrCreate(KVOptions{
        {},
        {std::make_shared<SpyFileSystem>(std::make_shared<PosixFileSystem>(temp_dir.path()))},
        {},
        {},
        {},
    });
    REQUIRE(!kv_or);
    REQUIRE(kv_or.err().code == KVErrorCodes::InvalidArguments);
}

SCENARIO("I cannot put invalid inputs to the map", "[kv]") {
    auto temp_dir = TempDir();
    auto kv_or = open_kv(temp_dir.path());
    REQUIRE(kv_or);
    auto kv = std::move(kv_or.val());

    {
        auto e = kv->put("", BorrowedSlice{""});
        REQUIRE(e.code == KVErrorCodes::InvalidArguments);
        REQUIRE(e.msg.find("empty") != std::string::npos);
    }

    {
        std::string key{};
        key.resize(UINT32_MAX);
        auto e = kv->put(key, BorrowedSlice{""});
        REQUIRE(e.code == KVErrorCodes::InvalidArguments);
        REQUIRE(e.msg.find("Key length") != std::string::npos);
    }

    {
        std::string key{};
        auto e = kv->put("a", BorrowedSlice{key.data(), UINT32_MAX});
        REQUIRE(e.code == KVErrorCodes::InvalidArguments);
        REQUIRE(e.msg.find("Value length") != std::string::npos);
    }
}

SCENARIO("I create a KV map with manual compaction", "[kv]") {
    auto temp_dir = TempDir();
    auto kv_or = open_kv_manual_compaction(temp_dir.path());
    REQUIRE(kv_or);
    auto kv = std::move(kv_or.val());

    std::vector<std::string> keys{};
    auto key_gen = random(1, 512);
    for (int i = 0; i < 20; i++) {
        keys.emplace_back(key_gen.get());
        key_gen.next();
    }
    const std::string &value = GENERATE(take(1, random(1, 1 * 1024 * 1024)));

    // Put duplicated key-values so that we would benefit from compaction
    for (int i = 0; i < 10; i++) {
        for (const auto &k : keys) {
            auto e = kv->put(k, BorrowedSlice{value});
            REQUIRE(e);
        }
    }

    auto size_now = kv->currentSizeBytes();
    auto e = kv->compact();
    REQUIRE(e);
    // Ensure we got smaller
    REQUIRE(kv->currentSizeBytes() < size_now);
}

SCENARIO("I open a KV map from a shadow file", "[kv]") {
    auto temp_dir = TempDir();
    std::filesystem::create_directories(temp_dir.path());

    // map1 is a valid KV map which is truncated to 5K which makes the last value unreadable.
    // This will verify that we're still able to load the file and read as much as is uncorrupted.
    std::filesystem::copy_file(std::filesystem::path(__FILE__).parent_path() / "test_data" / "kv" / "map1.kv",
                               // identifier is test-kv-map, so the shadow file name is test-kv-maps
                               temp_dir.path() / "test-kv-maps");

    auto kv_or = open_kv(temp_dir.path());
    REQUIRE(kv_or);
    auto kv = std::move(kv_or.val());

    auto keys_or = kv->listKeys();
    REQUIRE(keys_or);
    auto keys = keys_or.val();
    REQUIRE(keys.size() == 1);
    REQUIRE(keys.front() == "a"s);
}

SCENARIO("I can detect a corrupt KV map value", "[kv]") {
    GIVEN("I create a KV map with multiple entries") {
        auto temp_dir = TempDir();
        auto kv_or = open_kv(temp_dir.path());
        REQUIRE(kv_or);

        auto kv = std::move(kv_or.val());

        // populate the map with random keys and values
        auto num_entries = 10;
        auto test_data = generate_key_values(num_entries);
        for (const auto &key_value : test_data) {
            auto e = kv->put(key_value.first, BorrowedSlice{key_value.second});
            REQUIRE(e);
            auto v_or = kv->get(key_value.first);
            REQUIRE(v_or);
            REQUIRE(std::string_view{v_or.val().char_data(), v_or.val().size()} == key_value.second);
        }

        WHEN("I corrupt the next to last entry's key") {
            std::fstream file(temp_dir.path() / "test-kv-map", std::ios::in | std::ios::out | std::ios::binary);
            REQUIRE(file);

            // entries are stored as [header, key, value]
            // seek next to last key and overwrite it
            auto offset = 0;
            for (auto i = 0; i < num_entries - 2; i++) {
                offset += sizeof(aws::gg::kv::detail::KVHeader);
                offset += test_data[i].first.size();
                offset += test_data[i].second.size();
            }

            file.seekp(offset + sizeof(aws::gg::kv::detail::KVHeader));

            std::string corrupted_key = "key";
            std::string non_corrupted_key = test_data[num_entries - 2].first;
            file.write(corrupted_key.c_str(), static_cast<std::streamsize>(corrupted_key.size()));
            file.close();

            THEN("Retrieving next to last entry succeeds") {
                auto v_or = kv->get(test_data[num_entries - 2].first);
                REQUIRE(v_or);
                REQUIRE(std::string_view{v_or.val().char_data(), v_or.val().size()} == test_data[num_entries - 2].second);

                AND_WHEN("I reset the store") {
                    kv.reset();
                    kv_or = open_kv(temp_dir.path());
                    REQUIRE(kv_or);
                    kv = std::move(kv_or.val());

                    THEN("KeyNotFound for corrupted Key and rest of the store is untouched") {
                        for (auto i = 0; i < num_entries; i++) {
                            if (i == num_entries - 2) {
                                v_or = kv->get(test_data[i].first);
                                REQUIRE(!v_or);
                                REQUIRE(v_or.err().code == KVErrorCodes::KeyNotFound);
                            } else {
                                v_or = kv->get(test_data[i].first);
                                REQUIRE(v_or);
                                REQUIRE(v_or.val().string() == test_data[i].second);
                            }
                        }
                    }
                }
            }
        }

        WHEN("I corrupt the next to last entry's value") {
            std::fstream file(temp_dir.path() / "test-kv-map", std::ios::in | std::ios::out | std::ios::binary);
            REQUIRE(file);

            // entries are stored as [header, key, value]
            // seek next to last value and overwrite it
            auto offset = 0;
            for (auto i = 0; i < num_entries - 2; i++) {
                offset += sizeof(aws::gg::kv::detail::KVHeader);
                offset += test_data[i].first.size();
                offset += test_data[i].second.size();
            }
            file.seekp(offset + sizeof(aws::gg::kv::detail::KVHeader) + test_data[num_entries - 1].first.size());

            std::string corrupted_value = "value";
            file.write(corrupted_value.c_str(), static_cast<std::streamsize>(corrupted_value.size()));
            file.close();

            THEN("Retrieving next to last entry fails") {
                auto v_or = kv->get(test_data[num_entries - 2].first);
                REQUIRE(!v_or);
                REQUIRE(v_or.err().code == KVErrorCodes::DataCorrupted);

                AND_WHEN("I reset the store") {
                    kv.reset();
                    kv_or = open_kv(temp_dir.path());
                    REQUIRE(kv_or);
                    kv = std::move(kv_or.val());

                    THEN("Corrupted entry and subsequent entries are removed") {
                        for (auto i = num_entries - 2; i < num_entries; i++) {
                            v_or = kv->get(test_data[i].first);
                            REQUIRE(!v_or);
                            REQUIRE(v_or.err().code == KVErrorCodes::KeyNotFound);
                        }

                        AND_THEN("Rest of store is untouched") {
                            for (auto i = 0; i < num_entries - 2; i++) {
                                v_or = kv->get(test_data[i].first);
                                REQUIRE(v_or);
                                REQUIRE(v_or.val().string() == test_data[i].second);
                            }
                        }
                    }
                }
            }
        }
    }
}

SCENARIO("KV store with multiple values per key is corrupted", "[kv]") {
    GIVEN("I create an uncompacted KV map containing a key with multiple values") {
        auto temp_dir = TempDir();
        auto kv_or = open_kv(temp_dir.path());
        REQUIRE(kv_or);

        auto kv = std::move(kv_or.val());

        auto num_unique_keys = 2;

        auto test_data = generate_key_values(num_unique_keys);
        for (const auto &key_value : test_data) {
            auto e = kv->put(key_value.first, BorrowedSlice{key_value.second});
            REQUIRE(e);
            auto v_or = kv->get(key_value.first);
            REQUIRE(v_or);
            REQUIRE(std::string_view{v_or.val().char_data(), v_or.val().size()} == key_value.second);
        }

        // overwrite the values for each key
        for (const auto &key_value : test_data) {
            auto e = kv->put(key_value.first, BorrowedSlice{"overwritten"});
            REQUIRE(e);
            auto v_or = kv->get(key_value.first);
            REQUIRE(v_or);
            REQUIRE(std::string_view{v_or.val().char_data(), v_or.val().size()} == "overwritten");
        }

        WHEN("I corrupt the first value") {
            std::fstream file(temp_dir.path() / "test-kv-map", std::ios::in | std::ios::out | std::ios::binary);
            REQUIRE(file);

            // entries are stored as [header, key, value]
            auto offset = 0;
            for (auto i = 0; i < num_unique_keys; i++) {
                offset += sizeof(aws::gg::kv::detail::KVHeader);
                offset += test_data[i].first.size();
                offset += test_data[i].second.size();
            }
            file.seekp(offset);

            std::string corrupted_header = "A";
            file.write(corrupted_header.c_str(), static_cast<std::streamsize>(corrupted_header.size()));
            file.close();

            AND_WHEN("I reset the store") {
                kv.reset();
                kv_or = open_kv(temp_dir.path());
                REQUIRE(kv_or);
                kv = std::move(kv_or.val());

                THEN("Old values are returned for the keys") {
                    for (const auto &key_value : test_data) {
                        auto v_or = kv->get(key_value.first);
                        REQUIRE(v_or);
                        REQUIRE(std::string_view{v_or.val().char_data(), v_or.val().size()} == key_value.second);
                    }
                }
            }

            AND_WHEN("I compact the store") {
                REQUIRE(kv->compact());

                THEN("The corrupted key is removed from the store") {
                    auto v_or = kv->get(test_data[0].first);
                    REQUIRE(!v_or);
                    REQUIRE(v_or.err().code == KVErrorCodes::KeyNotFound);

                    AND_THEN("The uncorrupted key is still in the store and points to newest value") {
                        v_or = kv->get(test_data[1].first);
                        REQUIRE(v_or);
                        REQUIRE(v_or.val().string() == "overwritten");
                    }
                }
            }
        }
    }
}

SCENARIO("I can detect a corrupt KV map header", "[kv]") {
    GIVEN("I create a KV map with multiple entries") {
        auto temp_dir = TempDir();
        auto kv_or = open_kv(temp_dir.path());
        REQUIRE(kv_or);

        auto kv = std::move(kv_or.val());

        // populate the map with random keys and values
        auto num_entries = 10;
        auto test_data = generate_key_values(num_entries);
        for (const auto &key_value : test_data) {
            auto e = kv->put(key_value.first, BorrowedSlice{key_value.second});
            REQUIRE(e);
            auto v_or = kv->get(key_value.first);
            REQUIRE(v_or);
            REQUIRE(std::string_view{v_or.val().char_data(), v_or.val().size()} == key_value.second);
        }

        WHEN("I corrupt the next to last entry's header") {
            std::fstream file(temp_dir.path() / "test-kv-map", std::ios::in | std::ios::out | std::ios::binary);
            REQUIRE(file);

            // entries are stored as [header, key, value]
            // seek to last header and overwrite it
            auto offset = 0;
            for (auto i = 0; i < num_entries - 2; i++) {
                offset += sizeof(aws::gg::kv::detail::KVHeader);
                offset += test_data[i].first.size();
                offset += test_data[i].second.size();
            }
            file.seekp(offset);

            std::string corrupted_header = "A";
            file.write(corrupted_header.c_str(), static_cast<std::streamsize>(corrupted_header.size()));
            file.close();

            THEN("Retrieving next to last entry fails") {
                auto v_or = kv->get(test_data[num_entries - 2].first);
                REQUIRE(!v_or);
                REQUIRE(v_or.err().code == KVErrorCodes::HeaderCorrupted);

                AND_WHEN("I reset the store") {
                    kv.reset();
                    kv_or = open_kv(temp_dir.path());
                    REQUIRE(kv_or);
                    kv = std::move(kv_or.val());

                    THEN("Corrupted entry and subsequent entries are removed") {
                        for (auto i = num_entries - 2; i < num_entries; i++) {
                            v_or = kv->get(test_data[i].first);
                            REQUIRE(!v_or);
                            REQUIRE(v_or.err().code == KVErrorCodes::KeyNotFound);
                        }

                        AND_THEN("Rest of store is untouched") {
                            for (auto i = 0; i < num_entries - 2; i++) {
                                v_or = kv->get(test_data[i].first);
                                REQUIRE(v_or);
                                REQUIRE(v_or.val().string() == test_data[i].second);
                            }
                        }
                    }
                }
            }
        }
    }
}

SCENARIO("I can create a KV map", "[kv]") {
    GIVEN("I create an empty KV map") {
        auto temp_dir = TempDir();
        auto kv_or = open_kv(temp_dir.path());
        REQUIRE(kv_or);

        auto kv = std::move(kv_or.val());

        auto keys_or = kv->listKeys();
        REQUIRE(keys_or);
        REQUIRE(keys_or.val().empty());
        auto e = kv->put("key", BorrowedSlice{"value"});
        REQUIRE(e);

        keys_or = kv->listKeys();
        REQUIRE(keys_or);
        REQUIRE(!keys_or.val().empty());
        REQUIRE(keys_or.val()[0] == "key"s);

        e = kv->compact();
        REQUIRE(e);

        const std::string &key = GENERATE(take(10, random(1, 512)));
        const std::string &value = GENERATE(take(1, random(1, 1 * 1024 * 1024)));
        const std::string_view new_value = "new value"sv;

        WHEN("I add a value") {
            e = kv->put(key, BorrowedSlice{value});
            REQUIRE(e);

            THEN("I can get the value back") {
                auto v_or = kv->get(key);
                REQUIRE(v_or);
                REQUIRE(std::string_view{v_or.val().char_data(), v_or.val().size()} == value);

                AND_WHEN("I update the value") {
                    e = kv->put(key, BorrowedSlice{new_value.data(), static_cast<uint32_t>(new_value.size())});
                    REQUIRE(e);
                    THEN("I get the new value back") {
                        v_or = kv->get(key);
                        REQUIRE(v_or);
                        REQUIRE(std::string_view{v_or.val().char_data(), v_or.val().size()} == new_value);

                        e = kv->put(key, BorrowedSlice{value});
                        REQUIRE(e);

                        AND_WHEN("I close the KV and open it again") {
                            kv.reset();

                            kv_or = open_kv(temp_dir.path());
                            REQUIRE(kv_or);

                            kv = std::move(kv_or.val());

                            THEN("I can get the value back") {
                                v_or = kv->get(key);
                                REQUIRE(v_or);
                                REQUIRE(std::string_view{v_or.val().char_data(), v_or.val().size()} == value);

                                AND_WHEN("I remove the key") {
                                    e = kv->remove(key);
                                    REQUIRE(e);

                                    THEN("I can't get the value back") {
                                        v_or = kv->get(key);
                                        REQUIRE(!v_or);
                                        REQUIRE(v_or.err().code == KVErrorCodes::KeyNotFound);

                                        e = kv->remove("non-existent-key");
                                        REQUIRE(e.code == KVErrorCodes::KeyNotFound);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}