#pragma once
// Umbrella header for the shmctx library.
//
// shm_context.h is vendored verbatim (plus a guarded test hook) from
// core-libraries/include/shm_context.h. It uses std::vector / std::string /
// std::min without including their headers directly, so include them first.
#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "shmctx/macros.h"
#include "shmctx/shm_queue_types.h"
#include "shmctx/spin_lock.h"
#include "shmctx/shm_context.h"
