## Usage

To access these plugins, launch the `rqt_bag` GUI tool (see
[rqt_bag](https://index.ros.org/p/rqt_bag) for details), right-click on
the timeline of the desired topic select a view type from the context
menu.

![rqt_bag message views](doc/rqt_bag_views.png)

## Plugins

The following plugins are currently provided for `rqt_bag`:

### Image

The image view displays raw image data for
[sensor_msgs/msg/Image](https://docs.ros.org/en/rolling/p/sensor_msgs/msg/Image.html)
and
[sensor_msgs/msg/CompressedImage](https://docs.ros.org/en/rolling/p/sensor_msgs/msg/CompressedImage.html)
messages:

![rqt_bag image view](doc/image_grab.png)

Re-sizing the window scales the image.

### Plot

The plot view displays a time series plot of numeric fields in a ROS
message. The view inherits from the `rqt_plot` code base, so all
plotting options are the same as with `rqt_plot`.

![rqt_bag plot view](doc/plot_grab.png)

To plot a message field, select the field from the list in the right
window (Note: currently only fields from the same message topic can be
plotted in the same view window).

Clicking on the Configure Plot button in the toolbar opens a new window
for selecting the plotting backend and marker preferences.

By default, the plotted data is sub-sampled in time to minimize the cpu
load. To change the sampling resolution, deselect 'auto' in the
Resolution section and enter a sampling time in seconds. The plotted
data will automatically refresh with the new sampling time.
