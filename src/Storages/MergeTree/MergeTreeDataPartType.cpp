#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PART_TYPE;
}

void MergeTreeDataPartType::fromString(const String & str)
{
    if (str == "Wide")
        value = WIDE;
    else if (str == "Compact")
        value = COMPACT;
    else if (str == "InMemory")
        value = IN_MEMORY;
    else if (str == "InNVM")
        value =IN_NVM;
    else if (str == "PMCS")
        value =PMCS;
    else
        throw DB::Exception("Unexpected string for part type: " + str, ErrorCodes::UNKNOWN_PART_TYPE);
}

String MergeTreeDataPartType::toString() const
{
    switch (value)
    {
        case WIDE:
            return "Wide";
        case COMPACT:
            return "Compact";
        case IN_MEMORY:
            return "InMemory";
        case IN_NVM:
            return "InNVM";
        case PMCS:
            return "PMCS";
        default:
            return "Unknown";
    }
}

}
