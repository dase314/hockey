#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Poco/Path.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_PART_TYPE;
}

std::optional<std::string> MergeTreeIndexGranularityInfo::getMarksExtensionFromFilesystem(const DiskPtr & disk, const String & path_to_part)
{
    if (disk->exists(path_to_part))
    {
        for (DiskDirectoryIteratorPtr it = disk->iterateDirectory(path_to_part); it->isValid(); it->next())
        {
            Poco::Path path(it->path());
            const auto & ext = "." + path.getExtension();
            if (ext == getNonAdaptiveMrkExtension()
                || ext == getAdaptiveMrkExtension(MergeTreeDataPartType::WIDE)
                || ext == getAdaptiveMrkExtension(MergeTreeDataPartType::COMPACT))
                return ext;
        }
    }
    return {};
}

MergeTreeIndexGranularityInfo::MergeTreeIndexGranularityInfo(const MergeTreeData & storage, MergeTreeDataPartType type_)
    : type(type_)
{
    const auto storage_settings = storage.getSettings();
    fixed_index_granularity = storage_settings->index_granularity;

    /// Granularity is fixed
    if (!storage.canUseAdaptiveGranularity())
    {
        if (type != MergeTreeDataPartType::WIDE && type != MergeTreeDataPartType::IN_NVM && type != MergeTreeDataPartType::PMCS)
            throw Exception("Only Wide parts can be used with non-adaptive granularity."+type.toString(), ErrorCodes::NOT_IMPLEMENTED);
        setNonAdaptive();
    }
    else
        setAdaptive(storage_settings->index_granularity_bytes);
}

void MergeTreeIndexGranularityInfo::changeGranularityIfRequired(const DiskPtr & disk, const String & path_to_part)
{
    auto mrk_ext = getMarksExtensionFromFilesystem(disk, path_to_part);
    if (mrk_ext && *mrk_ext == getNonAdaptiveMrkExtension())
        setNonAdaptive();
}

void MergeTreeIndexGranularityInfo::setAdaptive(size_t index_granularity_bytes_)
{
    is_adaptive = true;
    marks_file_extension = getAdaptiveMrkExtension(type);
    index_granularity_bytes = index_granularity_bytes_;
}

void MergeTreeIndexGranularityInfo::setNonAdaptive()
{
    is_adaptive = false;
    marks_file_extension = getNonAdaptiveMrkExtension();
    index_granularity_bytes = 0;
}

void MergeTreeIndexGranularityInfo::setNonAdaptivePmem()
{
    is_adaptive = false;
    marks_file_extension = "";
    index_granularity_bytes = 0;
}

size_t MergeTreeIndexGranularityInfo::getMarkSizeInBytes(size_t columns_num) const
{
    if (type == MergeTreeDataPartType::WIDE)
        return is_adaptive ? getAdaptiveMrkSizeWide() : getNonAdaptiveMrkSizeWide();
    else if (type == MergeTreeDataPartType::COMPACT)
        return getAdaptiveMrkSizeCompact(columns_num);
    else if (type == MergeTreeDataPartType::IN_MEMORY)
        return 0;
    else if (type == MergeTreeDataPartType::IN_NVM)
        return is_adaptive ? getAdaptiveMrkSizeWide() : getNonAdaptiveMrkSizeWide();
    else if (type == MergeTreeDataPartType::PMCS)
        return 0;
    else
        throw Exception("Unknown part type", ErrorCodes::UNKNOWN_PART_TYPE);
}

size_t getAdaptiveMrkSizeCompact(size_t columns_num)
{
    /// Each mark contains number of rows in granule and two offsets for every column.
    return sizeof(UInt64) * (columns_num * 2 + 1);
}

std::string getAdaptiveMrkExtension(MergeTreeDataPartType part_type)
{
    if (part_type == MergeTreeDataPartType::WIDE ||part_type == MergeTreeDataPartType::IN_NVM)
        return ".mrk2";
    else if (part_type == MergeTreeDataPartType::COMPACT)
        return ".mrk3";
    else if (part_type == MergeTreeDataPartType::IN_MEMORY)
        return "";
    else if (part_type == MergeTreeDataPartType::PMCS)
        return "";
    else
        throw Exception("Unknown part type", ErrorCodes::UNKNOWN_PART_TYPE);
}

}
