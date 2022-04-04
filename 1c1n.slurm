#!/bin/bash
#SBATCH --time=01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --output=1n1c.out

module load foss/2019b
module load python/3.7.4
module load gcc/8.3.0
module load openmpi/3.1.4

time mpirun python3 __(filename)____.py