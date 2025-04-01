%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internationalizing Like China


% Master File to Run the following scripts:

% heterogeneous_figure.m: Produces the four panels of Figure 6. Output to the figs subfolder as multiple_a.eps, multiple_b.eps, multiple_c.eps, multiple_d.eps

% homogeneous_figure.m: Produces the four panels of Figure A.XIV. Output to the figs subfolder as single_a.eps, single_b.eps, single_c.eps, single_d.eps

% heterogeneous_demand_figure.m: Produces the four panels of Figure A.XV. Output to the figs subfolder as multiple_hd_a.eps, multiple_hd_b.eps, multiple_hd_c.eps, multiple_hd_d.eps

% Feb 2024
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

clear all
close all
clc

run('4_theory/heterogeneous_figure.m')

run('4_theory/homogeneous_figure.m')

run('4_theory/heterogeneous_demand_figure.m')