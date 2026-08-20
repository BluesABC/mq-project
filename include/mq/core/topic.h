#pragma once

#include <string_view>

namespace mq::core {

bool IsValidTopicName(std::string_view topic);

}  // namespace mq::core
