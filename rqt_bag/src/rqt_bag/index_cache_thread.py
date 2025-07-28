# Copyright (c) 2012, Willow Garage, Inc.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#
#    * Neither the name of the Willow Garage nor the names of its
#      contributors may be used to endorse or promote products derived from
#      this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

import threading
import time


class IndexCacheThread(threading.Thread):
    """
    Updates invalid caches.

    One thread per timeline.
    """

    def __init__(self, timeline):
        threading.Thread.__init__(self)
        self.timeline = timeline
        self._stop_flag = False
        self.setDaemon(True)
        self.start()

    def run(self):
        # Delay start of the indexing so that the basic UI has time to be loaded
        time.sleep(2.0)

        while not self._stop_flag:
            with self.timeline.index_cache_cv:
                # Wait until the cache is dirty
                while len(self.timeline.invalidated_caches) == 0:
                    self.timeline.index_cache_cv.wait()
                    if self._stop_flag:
                        return

                # Update the index for all invalidated topics
                def progress_cb(progress: int) -> None:
                    if not self._stop_flag:
                        self.timeline.scene().background_progress = progress
                        self.timeline.scene().status_bar_changed_signal.emit()

                topics = self.timeline.invalidated_caches.intersection(set(self.timeline.topics))
                updated = (self.timeline._update_index_cache(topics, progress_cb) > 0)

            if updated:
                self.timeline.scene().background_progress = 0
                self.timeline.scene().status_bar_changed_signal.emit()
                self.timeline.scene().update()
                # Give the GUI some time to update
                time.sleep(1.0)

    def stop(self):
        self._stop_flag = True
        cv = self.timeline.index_cache_cv
        with cv:
            cv.notify()
