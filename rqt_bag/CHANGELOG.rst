^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Changelog for package rqt_bag
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1.5.5 (2025-08-06)
------------------
* Display roll, pitch, yaw values for quaternions (backport `#179 <https://github.com/ros-visualization/rqt_bag/issues/179>`_) (`#199 <https://github.com/ros-visualization/rqt_bag/issues/199>`_)
  * Display roll, pitch, yaw values for quaternions (`#179 <https://github.com/ros-visualization/rqt_bag/issues/179>`_)
  (cherry picked from commit 4593d82d2b23a322119c6a518befec970db7d54e)
* Improved raw view to better handle arrays and time objects (backport `#173 <https://github.com/ros-visualization/rqt_bag/issues/173>`_) (`#197 <https://github.com/ros-visualization/rqt_bag/issues/197>`_)
* plot_view: Fixed display of initial message (backport `#180 <https://github.com/ros-visualization/rqt_bag/issues/180>`_) (`#187 <https://github.com/ros-visualization/rqt_bag/issues/187>`_)
  plot_view: Fixed display of initial message (`#180 <https://github.com/ros-visualization/rqt_bag/issues/180>`_)
  (cherry picked from commit 9ff337266e97143c2ec9eef6f9aa03bde2b31997)
  Co-authored-by: Martin Pecka <peci1@seznam.cz>
* Contributors: Martin Pecka, mergify[bot]

1.5.4 (2024-09-06)
------------------
* Updated player QoS (backport `#164 <https://github.com/ros-visualization/rqt_bag/issues/164>`_) (`#167 <https://github.com/ros-visualization/rqt_bag/issues/167>`_)
  Updated player QoS (`#164 <https://github.com/ros-visualization/rqt_bag/issues/164>`_)
  (cherry picked from commit 4a9a69617e439140771a2b292aaa4aa93a213a03)
  Co-authored-by: Alejandro Hernández Cordero <ahcorde@gmail.com>
* Contributors: mergify[bot]

1.5.3 (2024-05-14)
------------------
* Adapted to rosbag2_py (backport `#156 <https://github.com/ros-visualization/rqt_bag/issues/156>`_) (`#165 <https://github.com/ros-visualization/rqt_bag/issues/165>`_)
  Adapted to rosbag2_py (`#156 <https://github.com/ros-visualization/rqt_bag/issues/156>`_)
  * Switch to an in-built ImageQt class.
  That's because in newer versions of PIL, they skip over
  PyQt5 support even though it works.
  * Update for new rosbag2_py API.
  * Fix TopicMetadata call
  * Avoid freeze the gui
  * Fixed checkboes
  * Update with the license from PIL.
  Note that this is an OSI-approved license, even though
  it is technically deprecated by
  OSI: https://opensource.org/license/historical-php
  * Only change the checkbox when needed.
  Otherwise, we end up with endless paint() calls which
  hammer the CPU.
  * A few small fixes for PIL.
  (cherry picked from commit e7879325f075c9d6749a4324618691933727574c)
  Co-authored-by: Alejandro Hernández Cordero <ahcorde@gmail.com>
* Fixed button icons (backport `#159 <https://github.com/ros-visualization/rqt_bag/issues/159>`_) (`#160 <https://github.com/ros-visualization/rqt_bag/issues/160>`_)
  Fixed button icons (`#159 <https://github.com/ros-visualization/rqt_bag/issues/159>`_)
  (cherry picked from commit 0d386692f63e35a8ec3c61dee0d70c389f89bf2d)
  Co-authored-by: Alejandro Hernández Cordero <ahcorde@gmail.com>
* Contributors: mergify[bot]

1.5.2 (2024-03-28)
------------------
* Add in copyright tests to rqt_bag. (`#154 <https://github.com/ros-visualization/rqt_bag/issues/154>`_)
* Add a test dependency on pytest. (`#153 <https://github.com/ros-visualization/rqt_bag/issues/153>`_)
* Revert "Add a dependency on pytest to rqt_bag and rqt_bag_plugins. (#… (`#151 <https://github.com/ros-visualization/rqt_bag/issues/151>`_)
* Update maintainer to myself. (`#150 <https://github.com/ros-visualization/rqt_bag/issues/150>`_)
* Update maintainer list in package.xml files (`#149 <https://github.com/ros-visualization/rqt_bag/issues/149>`_)
* Contributors: Chris Lalancette, Michael Jeronimo

1.5.1 (2024-02-07)
------------------
* Add a dependency on pytest to rqt_bag and rqt_bag_plugins. (`#148 <https://github.com/ros-visualization/rqt_bag/issues/148>`_)
* Contributors: Chris Lalancette

1.5.0 (2023-06-07)
------------------
* [ros2] Enable Save (`#142 <https://github.com/ros-visualization/rqt_bag/issues/142>`_)
* Call close (`#141 <https://github.com/ros-visualization/rqt_bag/issues/141>`_)
* Contributors: Yadu

1.4.1 (2023-05-11)
------------------
* Use default storage id (`#139 <https://github.com/ros-visualization/rqt_bag/issues/139>`_)
* Contributors: Yadunund

1.4.0 (2023-04-28)
------------------

1.3.1 (2023-04-11)
------------------

1.3.0 (2023-02-14)
------------------
* Use rosbag2_py API instead of direct bag parsing
* [rolling] Update maintainers - 2022-11-07 (`#132 <https://github.com/ros-visualization/rqt_bag/issues/132>`_)
* For get_entry_after, bump by 1 nanosecond otherwise always get the same message equal to the timestamp
* Use rosbag2_py.reader for all message queries, remove sqlite3 direct usage
* Cleanup for review
* Improved logging
* Use a rosbag2_py.Reader to get bag metadata
* Disable reading from bag while recording - use direct caching to index for timeline
* Contributors: Audrow Nash, Emerson Knapp

1.2.1 (2022-09-13)
------------------
* Increase publishing checkbox size (`#122 <https://github.com/ros-visualization/rqt_bag/issues/122>`_)
* Fix toggle thumbnails button (`#117 <https://github.com/ros-visualization/rqt_bag/issues/117>`_)
* ensure data types match what PyQt expects (`#118 <https://github.com/ros-visualization/rqt_bag/issues/118>`_)
* Visualize topics being published and highlight topic being selected (`#116 <https://github.com/ros-visualization/rqt_bag/issues/116>`_)
* Be able to scroll up and down, not only zoom-in and out the timeline (`#114 <https://github.com/ros-visualization/rqt_bag/issues/114>`__)
* [Fixes] Fix crash when no qos metadata, make scroll bar appear if needed, add gitignore (`#113 <https://github.com/ros-visualization/rqt_bag/issues/113>`_)
* Contributors: Ivan Santiago Paunovic, Kenji Brameld

1.2.0 (2022-05-10)
------------------
* Fix the types being passed into QFont and QColor. (`#109 <https://github.com/ros-visualization/rqt_bag/issues/109>`_)
* Contributors: Chris Lalancette

1.1.2 (2022-02-06)
------------------
* Fix: rqt-bag fails to open bag specified on command-line (`#106 <https://github.com/ros-visualization/rqt_bag/issues/106>`_)
* Contributors: Michael Jeronimo

1.1.1 (2021-04-12)
------------------
* Remove an invalid import statement (`#101 <https://github.com/ros-visualization/rqt_bag/issues/101>`_)
* Contributors: Michael Jeronimo

1.1.0 (2021-03-18)
------------------
* Reset timeline zoom after loading a new bag. (`#98 <https://github.com/ros-visualization/rqt_bag/issues/98>`_)
* Refactor the Rosbag2 class (`#91 <https://github.com/ros-visualization/rqt_bag/issues/91>`_)
* Contributors: Michael Grupp, Michael Jeronimo

1.0.0 (2020-11-19)
------------------
* Fix exec_depend (`#89 <https://github.com/ros-visualization/rqt_bag/issues/89>`_)
* Use updated HistoryPolicy values to avoid deprecation warnings (`#88 <https://github.com/ros-visualization/rqt_bag/issues/88>`_)
* Enable recording for ROS2 (`#87 <https://github.com/ros-visualization/rqt_bag/issues/87>`_)
* Enable the playback functionality for ROS2 (`#85 <https://github.com/ros-visualization/rqt_bag/issues/85>`_)
* Port the topic and node selection dialogs to ROS2 (`#86 <https://github.com/ros-visualization/rqt_bag/issues/86>`_)
* Save the serialization format and offered_qos_profiles when exporting (`#84 <https://github.com/ros-visualization/rqt_bag/issues/84>`_)
* Enable the export/save bag functionality for ROS2 (`#82 <https://github.com/ros-visualization/rqt_bag/issues/82>`_)
* Update known message types and associated colors (`#81 <https://github.com/ros-visualization/rqt_bag/issues/81>`_)
* Open the bag directory instead of a single file (`#80 <https://github.com/ros-visualization/rqt_bag/issues/80>`_)
* Port the image_view plugin to ROS2 (`#78 <https://github.com/ros-visualization/rqt_bag/issues/78>`_)
* Clean up widgets in plot_view layout correctly (`#69 <https://github.com/ros-visualization/rqt_bag/issues/69>`_) (`#77 <https://github.com/ros-visualization/rqt_bag/issues/77>`_)
* Fix tuples for bisect calls (`#67 <https://github.com/ros-visualization/rqt_bag/issues/67>`_) (`#76 <https://github.com/ros-visualization/rqt_bag/issues/76>`__)
* Fix issue: no vertical scroll bar until window is resized (`#63 <https://github.com/ros-visualization/rqt_bag/issues/63>`_) (`#75 <https://github.com/ros-visualization/rqt_bag/issues/75>`_)
* Update the basic plugins for ROS2 (`#72 <https://github.com/ros-visualization/rqt_bag/issues/72>`_)
* Update the rosbag2 python module (`#71 <https://github.com/ros-visualization/rqt_bag/issues/71>`_)
* Dynamically resize the timeline when recording (`#66 <https://github.com/ros-visualization/rqt_bag/issues/66>`_)
* Starting point for resuming the ROS2 port (`#70 <https://github.com/ros-visualization/rqt_bag/issues/70>`_)
* Fix a bug with the status line progress bar (`#62 <https://github.com/ros-visualization/rqt_bag/issues/62>`_)
* Update a few minor status bar-related items (`#61 <https://github.com/ros-visualization/rqt_bag/issues/61>`_)
* Make the tree controls in the Raw View and Plot View consistent (`#57 <https://github.com/ros-visualization/rqt_bag/issues/57>`_)
* Update the package.xml files with the latest Open Robotics maintainers (`#58 <https://github.com/ros-visualization/rqt_bag/issues/58>`_)
* Contributors: Chris Lalancette, Michael Jeronimo

0.4.15 (2020-08-21)
-------------------
* fix Python 3 issue: long/int (`#52 <https://github.com/ros-visualization/rqt_bag/issues/52>`_)

0.4.14 (2020-08-07)
-------------------
* save last directory opened to load a bag file (`#40 <https://github.com/ros-visualization/rqt_bag/issues/40>`_)
* fix shebang line for Python 3 (`#48 <https://github.com/ros-visualization/rqt_bag/issues/48>`_)
* bump CMake minimum version to avoid CMP0048 warning

0.4.13 (2020-03-17)
-------------------
* fix Python 3 exception, wrap filter call in list() (`#46 <https://github.com/ros-visualization/rqt_bag/issues/46>`_)
* add Python 3 conditional dependencies (`#44 <https://github.com/ros-visualization/rqt_bag/issues/44>`_)
* autopep8 (`#30 <https://github.com/ros-visualization/rqt_bag/issues/30>`_)

0.4.12 (2018-03-21)
-------------------
* add support for opening multiple bag files at once (`#25 <https://github.com/ros-visualization/rqt_bag/issues/25>`_)
* fix debug/warning messages for unicode filenames (`#26 <https://github.com/ros-visualization/rqt_bag/issues/26>`_)

0.4.11 (2017-11-01)
-------------------
* fix regression from version 0.4.10 (`#17 <https://github.com/ros-visualization/rqt_bag/issues/17>`_)

0.4.10 (2017-10-25)
-------------------
* fix regression from version 0.4.9 (`#16 <https://github.com/ros-visualization/rqt_bag/issues/16>`_)

0.4.9 (2017-10-12)
------------------
* handle errors happening while loading a bag (`#14 <https://github.com/ros-visualization/rqt_bag/issues/14>`_)

0.4.8 (2017-04-24)
------------------
* add rqt_bag.launch file (`#440 <https://github.com/ros-visualization/rqt_common_plugins/pull/440>`_)

0.4.7 (2017-03-02)
------------------

0.4.6 (2017-02-27)
------------------

0.4.5 (2017-02-03)
------------------
* fix Python 2 regression from version 0.4.4 (`#424 <https://github.com/ros-visualization/rqt_common_plugins/issues/424>`_)

0.4.4 (2017-01-24)
------------------
* use Python 3 compatible syntax (`#421 <https://github.com/ros-visualization/rqt_common_plugins/pull/421>`_)
* fix race condition reading bag files (`#412 <https://github.com/ros-visualization/rqt_common_plugins/pull/412>`_)

0.4.3 (2016-11-02)
------------------

0.4.2 (2016-09-19)
------------------
* add "From nodes" button to record mode (`#348 <https://github.com/ros-visualization/rqt_common_plugins/issues/348>`_)
* show file size of bag file in the status bar (`#347 <https://github.com/ros-visualization/rqt_common_plugins/pull/347>`_)

0.4.1 (2016-05-16)
------------------
* fix mouse wheel delta in Qt 5 (`#376 <https://github.com/ros-visualization/rqt_common_plugins/issues/376>`_)

0.4.0 (2016-04-27)
------------------
* Support Qt 5 (in Kinetic and higher) as well as Qt 4 (in Jade and earlier) (`#359 <https://github.com/ros-visualization/rqt_common_plugins/pull/359>`_)
* fix publishing wrong topic after scrolling (`#362 <https://github.com/ros-visualization/rqt_common_plugins/pull/362>`_)

0.3.13 (2016-03-08)
-------------------
* RQT_BAG: Ensure monotonic clock publishing.
  Due to parallelism issues, a message can be published
  with a simulated timestamp in the past. This lead to
  undesired behaviors when using TF for example.
* Contributors: lsouchet

0.3.12 (2015-07-24)
-------------------
* Added step-by-step playback capability
* Contributors: Aaron Blasdel, sambrose

0.3.11 (2015-04-30)
-------------------
* fix viewer plugin relocation issue (`#306 <https://github.com/ros-visualization/rqt_common_plugins/issues/306>`_)

0.3.10 (2014-10-01)
-------------------
* fix topic type retrieval for multiple bag files (`#279 <https://github.com/ros-visualization/rqt_common_plugins/issues/279>`_)
* fix region_changed signal emission when no start/end stamps are set
* improve right-click menu
* improve popup management (`#280 <https://github.com/ros-visualization/rqt_common_plugins/issues/280>`_)
* implement recording of topic subsets
* sort the list of topics
* update plugin scripts to use full name to avoid future naming collisions

0.3.9 (2014-08-18)
------------------
* fix visibility with dark Qt theme (`#263 <https://github.com/ros-visualization/rqt_common_plugins/issues/263>`_)

0.3.8 (2014-07-15)
------------------

0.3.7 (2014-07-11)
------------------
* fix compatibility with Groovy, use queue_size for Python publishers only when available (`#243 <https://github.com/ros-visualization/rqt_common_plugins/issues/243>`_)
* use thread for loading bag files, emit region changed signal used by plotting plugin (`#239 <https://github.com/ros-visualization/rqt_common_plugins/issues/239>`_)
* export architecture_independent flag in package.xml (`#254 <https://github.com/ros-visualization/rqt_common_plugins/issues/254>`_)

0.3.6 (2014-06-02)
------------------
* fix closing and reopening topic views
* use queue_size for Python publishers

0.3.5 (2014-05-07)
------------------
* fix raw view not showing fields named 'msg' (`#226 <https://github.com/ros-visualization/rqt_common_plugins/issues/226>`_)

0.3.4 (2014-01-28)
------------------
* add option to publish clock tim from bag (`#204 <https://github.com/ros-visualization/rqt_common_plugins/issues/204>`_)

0.3.3 (2014-01-08)
------------------
* add groups for rqt plugins, renamed some plugins (`#167 <https://github.com/ros-visualization/rqt_common_plugins/issues/167>`_)
* fix high cpu load when idle (`#194 <https://github.com/ros-visualization/rqt_common_plugins/issues/194>`_)

0.3.2 (2013-10-14)
------------------

0.3.1 (2013-10-09)
------------------
* update rqt_bag plugin interface to work with qt_gui_core 0.2.18

0.3.0 (2013-08-28)
------------------
* fix rendering of icons on OS X (`ros-visualization/rqt#83 <https://github.com/ros-visualization/rqt/issues/83>`_)
* fix shutdown of plugin (`#31 <https://github.com/ros-visualization/rqt_common_plugins/issues/31>`_)
* fix saving parts of a bag (`#96 <https://github.com/ros-visualization/rqt_common_plugins/issues/96>`_)
* fix long topic names (`#114 <https://github.com/ros-visualization/rqt_common_plugins/issues/114>`__)
* fix zoom behavior (`#76 <https://github.com/ros-visualization/rqt_common_plugins/issues/76>`__)

0.2.17 (2013-07-04)
-------------------

0.2.16 (2013-04-09 13:33)
-------------------------

0.2.15 (2013-04-09 00:02)
-------------------------

0.2.14 (2013-03-14)
-------------------

0.2.13 (2013-03-11 22:14)
-------------------------

0.2.12 (2013-03-11 13:56)
-------------------------

0.2.11 (2013-03-08)
-------------------

0.2.10 (2013-01-22)
-------------------

0.2.9 (2013-01-17)
------------------
* Fix; skips time when resuming playback (`#5 <https://github.com/ros-visualization/rqt_common_plugins/issues/5>`_)
* Fix; timestamp printing issue (`#6 <https://github.com/ros-visualization/rqt_common_plugins/issues/6>`_)

0.2.8 (2013-01-11)
------------------
* expose command line arguments to rqt_bag script
* added fix to set play/pause button correctly when fastforwarding/rewinding, adjusted time headers to 0m00s instead of 0:00m for ease of reading
* support passing bagfiles on the command line (currently behind --args)

0.2.7 (2012-12-24)
------------------

0.2.6 (2012-12-23)
------------------

0.2.5 (2012-12-21 19:11)
------------------------

0.2.4 (2012-12-21 01:13)
------------------------

0.2.3 (2012-12-21 00:24)
------------------------

0.2.2 (2012-12-20 18:29)
------------------------

0.2.1 (2012-12-20 17:47)
------------------------

0.2.0 (2012-12-20 17:39)
------------------------
* first release of this package into Groovy
