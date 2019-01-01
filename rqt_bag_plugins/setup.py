from setuptools import setup

package_name = 'rqt_bag_plugins'
setup(
    name=package_name,
    version='0.4.12',
    package_dir={'': 'src'},
    packages=['rqt_bag_plugins'],
    data_files=[
        ('share/' + package_name + '/resource', ['resource/plot.ui']),
        ('share/' + package_name, ['plugin.xml']),
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml'])
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    author='Aaron Blasdel, Tim Field',
    maintainer='Dirk Thomas, Aaron Blasdel, Austin Hendrix',
    maintainer_email='dthomas@osrfoundation.org',
    keywords=['ROS'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Topic :: Software Development',
    ],
    description=(
        'rqt_bag provides a GUI plugin for displaying and replaying ROS bag files.'
    ),
    license='BSD',
)
