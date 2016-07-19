''' ---------------------------------------------------------------------
    Read detail.csv output from MSN offline experimentation pipeline
    and produces graphs of multiple (VW args, epsilon) curves over time.
    ---------------------------------------------------------------------
'''

import numpy as np
import matplotlib.pyplot as plt
import argparse
import sys
import seaborn as sns
import glob
import re
import os
import pylab

if __name__ == "__main__":
    arguments = sys.argv[1:]

sns.set(style="whitegrid", font_scale=1.2)

file = 'C:\\Users\\lhoang\\Downloads\\detail.csv'

args_to_plot = [
    '--quiet --cb_adf --rank_all --interact ud --cb_type mtr -l 0.02', 
    '--quiet --cb_adf --rank_all --interact ud --cb_type dr -l 0.1', 
    '--quiet --cb_adf --rank_all --interact ud --cb_type dr -l 0.005'
]

dict = {}
eps_list = {}
with open(file, 'r') as msnfile:
    line_no = 0
    for line in msnfile:
        line_no += 1
        if line_no == 1:
            continue
        data = line.split(',')
        time = data[2]
        args = data[4]
        eps  = float(data[5])
        loss = data[8]
        data_str = '{}\t{}\t{}\t{}'.format(time,args,eps,loss)
        
        if args not in dict:
            dict[args] = {}
        if eps not in dict[args]:
            dict[args][eps] = []
        dict[args][eps].append((time, loss))
        if eps not in eps_list:
            eps_list[eps] = True

for eps in eps_list:
    fig = plt.gcf()
    fig.set_size_inches(18.5, 10.5)
    for args in args_to_plot:
        x_axis = np.arange(len(dict[args][eps]))
        y_axis = [kv[1] for kv in dict[args][eps]]
        plt.plot(x_axis, y_axis, label='args = {}'.format(args[52:]))
    plt.title('Epsilon = {}'.format(eps))
    plt.legend(loc='upper right')
    plt.xlabel('time')
    plt.ylabel('loss')
    plt.savefig('eps-{}.png'.format(eps), dpi = 100)
    plt.close()

sys.exit()
