# s3cloud

**s3cloud** implements the Google Tensorflow's Filesystem Interface
  to let the Tensorflow support Amazon S3 Restful interface. 


##Install

```shell
git clone https://github.com/tensorflow/tensorflow.git
git clone git@github.com:wugang33/s3cloud.git
cp -r s3cloud tensorflow/tensorflow/core/platform
change the file
tensorflow/tensorflow/core/platform/default/build_config_root.bzl add
line  deps.append("//tensorflow/core/platform/s3cloud:s3_file_system")
under the line
deps.append("//tensorflow/core/platform/cloud:gcs_file_system") 
cd tensorflow;./configure;
bazel build --config opt //tensorflow/tools/pip_package:build_pip_package
bazel-bin/tensorflow/tools/pip_package/build_pip_package /tmp/tensorflow_pkg
sudo pip install /tmp/tensorflow_pkg/tensorflow-0.12.1-py2-none-any.whl
```


##Usage

```python
import tensorflow as tf


v1 = tf.Variable(tf.zeros([784,10]),name="v1")
v2 = tf.Variable(tf.zeros([10]),name="v2")
# Add an op to initialize the variables.
init_op = tf.initialize_all_variables()

# Add ops to save and restore all the variables.
saver = tf.train.Saver()

# Later, launch the model, initialize the variables, do some work, save the
# variables to disk.
with tf.Session() as sess:
    sess.run(init_op)
    save_path = saver.save(sess, "s3://BUCKET-mygod/test.ckpt")
    print "Model saved in file: ", save_path
```

```python
import tensorflow as tf

v1 = tf.Variable(tf.zeros([784,10]),name='v1')
v2 = tf.Variable(tf.zeros([10]),name='v2')
# Add an op to initialize the variables.
init_op = tf.initialize_all_variables()
# Add ops to save and restore all the variables.
saver = tf.train.Saver()

# Later, launch the model, use the saver to restore variables from disk, and
# do some work with the model.
with tf.Session() as sess:
    # Restore variables from disk.
    saver.restore(sess, "s3://BUCKET-mygod/test.ckpt")
    print "Model restored."
    # Do some work with the model
```
