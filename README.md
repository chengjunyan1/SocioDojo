# SocioDojo

This is the official repository for SocioDojo.



## Installation

1. First clone the directory. 

```code
git submodule init; git submodule update
```
(If showing error of no permission, need to first [add a new SSH key to your GitHub account](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account).)

2. Install dependencies.

Create a new environment using [conda](https://docs.conda.io/en/latest/miniconda.html), with Python >= 3.10.6 Install [PyTorch](https://pytorch.org/) (version >= 2.0.0). The repo is tested with PyTorch version of 1.10.1 and there is no guarentee that other version works. Then install other dependencies via:
```code
pip install -r requirements.txt
```

3. Download dataset.

Download the dataset here: https://drive.google.com/file/d/1YIk8mD7BMKqLr7EuZu6BZ1z16HUNpYZF/view?usp=sharing and unzip in the Env folder.




## Structure

Here we detail the repo's structure:
- [Agent](https://github.com/chengjunyan1/SocioDojo/tree/main/Agent): code for Analyst-Assistant-Actuator architecture and Hypothesis & Proof prompting
- [Env](https://github.com/chengjunyan1/SocioDojo/tree/main/Env): code for SocioDojo Corpus: Download the data above and unzip in this folder, it should has a structure like:
  - TS: time series
  - IS: information source
  - KB: knowledge base
- [run.py](https://github.com/chengjunyan1/SocioDojo/blob/main/run.py): the script for running experiments
- [config.py](https://github.com/chengjunyan1/SocioDojo/blob/main/config.py): you may edit configurations here


## Demo 

A demo is available [here](https://gam-gray.vercel.app/). The demo used minute-level financial data only.


## Citation
If you find our work and/or our code useful, please cite us via:

```bibtex
@inproceedings{
cheng2024sociodojo,
title={SocioDojo: Building Lifelong Analytical Agents with Real-world Text and Time Series},
author={Junyan Cheng and Peter Chin},
booktitle={The Twelfth International Conference on Learning Representations},
year={2024},
url={https://openreview.net/forum?id=s9z0HzWJJp}
}
```
