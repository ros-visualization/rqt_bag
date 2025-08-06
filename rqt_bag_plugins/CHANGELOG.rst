^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Changelog for package rqt_bag_plugins
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1.5.5 (2025-08-06)
------------------
* Display roll, pitch, yaw values for quaternions (backport `#179 <https://github.com/ros-visualization/rqt_bag/issues/179>`_) (`#199 <https://github.com/ros-visualization/rqt_bag/issues/199>`_)
  * Display roll, pitch, yaw values for quaternions (`#179 <https://github.com/ros-visualization/rqt_bag/issues/179>`_)
  (cherry picked from commit 4593d82d2b23a322119c6a518befec970db7d54e)
* Fixed image helper and added support for PNG-coded compressedDepth (backport `#176 <https://github.com/ros-visualization/rqt_bag/issues/176>`_)  (`#196 <https://github.com/ros-visualization/rqt_bag/issues/196>`_)
* Improve plot view (backport `#174 <https://github.com/ros-visualization/rqt_bag/issues/174>`_) (`#195 <https://github.com/ros-visualization/rqt_bag/issues/195>`_)
* plot_view: Fixed display of initial message (backport `#180 <https://github.com/ros-visualization/rqt_bag/issues/180>`_) (`#187 <https://github.com/ros-visualization/rqt_bag/issues/187>`_)
  plot_view: Fixed display of initial message (`#180 <https://github.com/ros-visualization/rqt_bag/issues/180>`_)
  (cherry picked from commit 9ff337266e97143c2ec9eef6f9aa03bde2b31997)
  Co-authored-by: Martin Pecka <peci1@seznam.cz>
* Contributors: Martin Pecka, mergify[bot]

1.5.4 (2024-09-06)
------------------

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
* Fixed image timeline renderer (backport `#158 <https://github.com/ros-visualization/rqt_bag/issues/158>`_) (`#163 <https://github.com/ros-visualization/rqt_bag/issues/163>`_)
  Fixed image timeline renderer (`#158 <https://github.com/ros-visualization/rqt_bag/issues/158>`_)
  (cherry picked from commit a939f27f6323ba1117edaafcdc498fc982743d4f)
  Co-authored-by: Alejandro Hernández Cordero <ahcorde@gmail.com>
* Contributors: mergify[bot]

1.5.2 (2024-03-28)
------------------
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

1.4.1 (2023-05-11)
------------------

1.4.0 (2023-04-28)
------------------

1.3.1 (2023-04-11)
------------------
* Changes the use of __slots_\_ for the field and field type getter (`#138 <https://github.com/ros-visualization/rqt_bag/issues/138>`_)
* Contributors: Eloy Briceno

1.3.0 (2023-02-14)
------------------
* [rolling] Update maintainers - 2022-11-07 (`#132 <https://github.com/ros-visualization/rqt_bag/issues/132>`_)
* Contributors: Audrow Nash

1.2.1 (2022-09-13)
------------------

1.2.0 (2022-05-10)
------------------

1.1.2 (2022-02-06)
------------------

1.1.1 (2021-04-12)
------------------

1.1.0 (2021-03-18)
------------------
* Refactor the Rosbag2 class (`#91 <https://github.com/ros-visualization/rqt_bag/issues/91>`_)
* Contributors: Michael Jeronimo

1.0.0 (2020-11-19)
------------------
* Port the plot view to ROS2 (`#79 <https://github.com/ros-visualization/rqt_bag/issues/79>`_)
* Port the image_view plugin to ROS2 (`#78 <https://github.com/ros-visualization/rqt_bag/issues/78>`_)
* Starting point for resuming the ROS2 port (`#70 <https://github.com/ros-visualization/rqt_bag/issues/70>`_)
* Make the tree controls in the Raw View and Plot View consistent (`#57 <https://github.com/ros-visualization/rqt_bag/issues/57>`_)
* Update the package.xml files with the latest Open Robotics maintainers (`#58 <https://github.com/ros-visualization/rqt_bag/issues/58>`_)
* initialize pil_mode when image is compressed (`#54 <https://github.com/ros-visualization/rqt_bag/issues/54>`_)
* Contributors: John Stechschulte, Michael Jeronimo

0.4.15 (2020-08-21)
-------------------

0.4.14 (2020-08-07)
-------------------
* support 16-bit bayer images (`#45 <https://github.com/ros-visualization/rqt_bag/issues/45>`_)
* maintain image aspect ratio (`#32 <https://github.com/ros-visualization/rqt_bag/issues/32>`_)
* fix Python 3 issue: long/int (`#51 <https://github.com/ros-visualization/rqt_bag/issues/51>`_)
* fix Python 3 issue: ensure str is encoded before decoding (`#50 <https://github.com/ros-visualization/rqt_bag/issues/50>`_)
* bump CMake minimum version to avoid CMP0048 warning

0.4.13 (2020-03-17)
-------------------
* add Python 3 conditional dependencies (`#44 <https://github.com/ros-visualization/rqt_bag/issues/44>`_)
* add cairocffi as the fallback module (`#43 <https://github.com/ros-visualization/rqt_bag/issues/43>`_)
* autopep8 (`#30 <https://github.com/ros-visualization/rqt_bag/issues/30>`_)

0.4.12 (2018-03-21)
-------------------

0.4.11 (2017-11-01)
-------------------

0.4.10 (2017-10-25)
-------------------

0.4.9 (2017-10-12)
------------------

0.4.8 (2017-04-24)
------------------

0.4.7 (2017-03-02)
------------------

0.4.6 (2017-02-27)
------------------

0.4.5 (2017-02-03)
------------------
* fix Python 2 regression from version 0.4.4 (`#426 <https://github.com/ros-visualization/rqt_common_plugins/issues/426>`_)

0.4.4 (2017-01-24)
------------------
* use Python 3 compatible syntax (`#421 <https://github.com/ros-visualization/rqt_common_plugins/pull/421>`_)

0.4.3 (2016-11-02)
------------------

0.4.2 (2016-09-19)
------------------
* fix crash when toggling thumbnail (`#380 <https://github.com/ros-visualization/rqt_common_plugins/issues/380>`_)
* lock bag when reading for plotting (`#382 <https://github.com/ros-visualization/rqt_common_plugins/pull/382>`_)

0.4.1 (2016-05-16)
------------------

0.4.0 (2016-04-27)
------------------
* Support Qt 5 (in Kinetic and higher) as well as Qt 4 (in Jade and earlier) (`#359 <https://github.com/ros-visualization/rqt_common_plugins/pull/359>`_)

0.3.13 (2016-03-08)
-------------------

0.3.12 (2015-07-24)
-------------------

0.3.11 (2015-04-30)
-------------------
* add missing dependency on rqt_plot (`#316 <https://github.com/ros-visualization/rqt_common_plugins/pull/316>`_)
* work around Pillow segfault if PyQt5 is installed (`#289 <https://github.com/ros-visualization/rqt_common_plugins/pull/289>`_, `#290 <https://github.com/ros-visualization/rqt_common_plugins/pull/290>`_)

0.3.10 (2014-10-01)
-------------------
* add displaying of depth image thumbnails

0.3.9 (2014-08-18)
------------------
* add missing dependency on python-cairo (`#269 <https://github.com/ros-visualization/rqt_common_plugins/issues/269>`_)

0.3.8 (2014-07-15)
------------------
* fix missing installation of resource subfolder

0.3.7 (2014-07-11)
------------------
* add plotting plugin (`#239 <https://github.com/ros-visualization/rqt_common_plugins/issues/239>`_)
* fix rqt_bag to plot array members (`#253 <https://github.com/ros-visualization/rqt_common_plugins/issues/253>`_)
* export architecture_independent flag in package.xml (`#254 <https://github.com/ros-visualization/rqt_common_plugins/issues/254>`_)

0.3.6 (2014-06-02)
------------------

0.3.5 (2014-05-07)
------------------
* fix PIL/Pillow error (`#224 <https://github.com/ros-visualization/rqt_common_plugins/issues/224>`_)

0.3.4 (2014-01-28)
------------------

0.3.3 (2014-01-08)
------------------

0.3.2 (2013-10-14)
------------------

0.3.1 (2013-10-09)
------------------

0.3.0 (2013-08-28)
------------------

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

0.2.8 (2013-01-11)
------------------

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
