def create_monochromatic_img(color, size):
    r = color[0] * np.ones((size[1], size[0], 1), dtype=np.uint8)
    g = color[1] * np.ones((size[1], size[0], 1), dtype=np.uint8)
    b = color[2] * np.ones((size[1], size[0], 1), dtype=np.uint8)
    return np.concatenate([r, g, b], axis=2)
import numpy as np
import cv2
import random
for b in range(1, 1000, 1):
    color = [random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)] #[r,g,b]
    size = [500,500] #[height,width]
    img = create_monochromatic_img(color, size)
    img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)
    cv2.imwrite('out'+str(b)+'.png', img)
    
