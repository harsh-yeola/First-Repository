# -*- coding: utf-8 -*-
"""
Created on Mon Dec 26 11:17:09 2022

@author: Dell
"""

import matplotlib.pyplot as plt
from PIL import Image
import numpy as np

img = Image.open(r'E:\Documents for employment\snap_20250208.jpg')
plt.imshow(img)

# size = 37,46
arr=np.array(img)
# img.thumbnail(size, Image.Resampling.LANCZOS)
fig=plt.figure(figsize=(5,5))
ax1=plt.subplot()
ax1.imshow(arr)
plt.savefig(r'E:\Documents for employment\snap_20250208_1.jpg')

fig,ax = plt.subplots(figsize=(5,5))
ax.imshow(img)
ax.spines['top'].set_visible(False)
ax.spines['left'].set_visible(False)
ax.spines['bottom'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.set_xticks([])
ax.set_yticks([])
plt.savefig(r'E:\Documents for employment\snap_20250208_2.jpg')