%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internationalizing Like China


% Code to produce Figure A.XIV: Equilibrium Reputation Cycle: Homogeneous
% Foreign Investors

% Feb 2024
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

clear all
close all
clc

%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Parameter definitions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%
maxIter = 50; % maximum iterations for loops

load('parameters/params1.mat') % model parameters


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Equations from the model describing the equilibrium
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

Mscr = @(M) 1 -(1-M)*taubar; % Denominator of interest rate schedule


% Verify debt level is positive
if Mscr(epsO) * gam * Q - Rbar < 0
    disp('Debt Level Negative')
    STOP
end

%%% Committed Type Values and Policies %%%
Dt = @(M) 2/b0 * 1./omeg(M) .* (Mscr(M)*gam*Q - Rbar); % Debt
Rt = @(M) 1/2*Rbar./Mscr(M) + 1/2*gam*Q;               % Interest rate             
I = @(M) A0 + Dt(M);                                   % Investment

n = @(tau) hf/(gam-(1-hf)/(1-tau)); % Net worth multiplier as a function of tau

V0 = @(M) n(0)*(gam*Q*I(M)-Rt(M).*Dt(M)); % Utility

g = n(taubar)/n(0); % g^f

rho = 1/beta*(g-1)/g; % rho^f

solTol = 1e-10; % Tolerance for solutions


% Find cut-off points for graduation, i.e., what ranges of M can be associated
% with a given graduation step
VU = V0(1-epsC); % Graduation Threshold in Utility
Ns = 1:25;

% Now, we project that back to the values V(M0) that can generate
% graduation at step N
vCutoff = (1-rho)./(1-rho.^(Ns+1))*VU;

% From here, define the initial reputation M0 associated with these values
% V(M0)
M0 = bisection_algorithm(@(M) vCutoff-V0(M),ones(1,length(Ns))*epsO,ones(1,length(Ns))*(1-epsC),maxIter); % Cutoffs in reputation

% Define the ranges of M for a graduation step
M0min = M0(2:end);
M0max = M0(1:end-1);
Ns = Ns(1:end-1);
int_size=M0min-M0max;




%%% Main Algorithm for Solving Begins %%%
%%% Loop through graduation steps to find a solution %%%

M0s = [];
terminal_gaps = [];
unique_check = [];
for n = Ns

    % Reputation range with this as a graduation step
    M0l = M0min(n);
    M0u = M0max(n);

    Vpath = @(m0) V0(m0) * (1-rho.^(1+[1:n]))/(1-rho); % This function describes the path of V(M_n) given M_0 for all steps up to candidate graduation step
    Mpath = @(m0) bisection_algorithm(@(M) Vpath(m0) - V0(M),ones(1,n)*epsO,ones(1,n)*(1-epsC),maxIter); % Given the above path of V(M_n) this inverts at each point in time to find the value M_n. Note that we know the function to be invertible.

    piN = @(m0) piTerminal(n,[m0,Mpath(m0)],epsO,epsC); % This function is derived from Bayesian updating. Given the path of Mn derived above, it finds the terminal probability piN
    

    VN = @(m0) V0(m0) * (1-rho^(n+1))/(1-rho); % This function finds the terminal flow value V0,T on graduation step
    
    
    MN = @(m0) bisection_algorithm(@(M) VN(m0) - V0(M),0,1,maxIter); % This function inverts V0,T into the value MT
    

    M0_out = bisection_algorithm(@(m0) piN(m0) - MN(m0),M0l,M0u,maxIter); % This function checks whether there is a value m0l<=M0<=m0u for which the terminal condition piT=MT is satisfied. This will fail for all but at most one interval (given the proof of uniqueness)
    
    if (piN(M0l) - MN(M0l))*(piN(M0u) - MN(M0u))<0
        unique_check(1,n) = 1;
    end 

    M0s = [M0s,M0_out]; % stores candidate solutions
    terminal_gaps = [terminal_gaps,piN(M0_out)-MN(M0_out)]; % stores the gap at terminal condition (which is zero for the equilibrium solution)

end

[~,loc] = min(abs(terminal_gaps)); % Finds the smallest possible gap and returns its location in the vector

if abs(terminal_gaps(loc)) < solTol
    disp('Solution Found')
    foundSolution = 1;
    fprintf('Terminal Conditon Numerical Error: %d \n',terminal_gaps(loc));
    fprintf('Opening-Up Date: %d \n',0)
    fprintf('Graduation Date: %d \n',loc)

    M0 = M0s(loc);          % Sets the equilibrium M0
    N = Ns(loc);            % Sets the equilibrium N
    
    if N == length(Ns)
        disp('Warning: Check for convergence of solution method. Error is potentially too high.');
    end
    
    if sum(unique_check) > 1
        disp('Warning: Multiple Numerical Solutions For immediate open up.');
    end
    
    Vpath = V0(M0) * (1-rho.^[2:N+1])/(1-rho);                                  % Computes the equilibrium path of utility
    Vpath_grad= [Vpath,ones(1,10)*VU];                                                  % Adds to the above a few steps after graduation
    Mpath = [M0,bisection_algorithm(@(M) Vpath - V0(M),zeros(1,N),ones(1,N),maxIter)];  % Computes the equilibrium path of M0
    Mpath_plot=[Mpath, ones(1,10)*(1-epsC)];                                            % Adds to the above a few steps after graduation
    piPath = piPathfcn(N,Mpath,epsO,epsC);                                              % Computes the equilibrium path of beliefs of investors about the committed type
    piPath_plot=[piPath,ones(1,10)*(1-epsC)];                                           % Adds to the above a few steps after graduation
    DtPath=Dt(Mpath);
    DtPath_plot=[DtPath, Dt(ones(1,10)*(1-epsC))];                                                       % Adds to the above a few steps after graduation
    RPath_plot = Rt(Mpath_plot);                                             % Computes the equilibrium path of interest rates
    mpath = (Mpath-piPath)./(1-piPath);                                                 % Computes the equilibrium path of mimicking probability
    mpath_plot=[mpath,zeros(1,10)];                                                     % Adds to the above a few steps after graduation
    nn = [0:N+10];
   
    

%%% The Following Code Generates the four panels of Figure A.XIV %%%

    figure;
        hold on
        plot(nn,Mpath_plot,'-o','LineWidth',6,'MarkerSize',10,'MarkerFaceColor','#0072BD')
        plot(nn,piPath_plot,':o','LineWidth',6,'MarkerSize',10,'MarkerFaceColor','#D95319')
        hold off
        axis([0 25 0 1.15])
        set(gca,'FontSize',14)
        xline(N,':r',{'Graduation','Step N'},'LineWidth',2.5,'FontSize',14,'Color',[0.6350 0.0780 0.1840],'LabelVerticalAlignment', 'bottom');
        yline(1-epsC,':k',{'Max','Reputation'},'LineWidth',2.5,'FontSize',14,'LabelHorizontalAlignment', 'left');
        xlabel('Step n', 'FontSize',18)
        legend({'M_{n}','\pi_n'}, 'Location','southeast','FontSize',14)
        legend boxoff
        box on
    print('../../output/appendix_figures/A_XIV_single_a','-depsc')

    figure;
    plot(nn,mpath_plot,'-o','LineWidth',6,'MarkerSize',10,'MarkerFaceColor','#0072BD')
        axis([0 25 -0.05 1])
        set(gca,'FontSize',14)
        xline(N,':r',{'Graduation','Step N'},'LineWidth',2.5,'FontSize',14,'Color',[0.6350 0.0780 0.1840],'LabelVerticalAlignment', 'top');
        xlabel('Step n', 'FontSize',18)
        yticks([0:0.1:1])
    print('../../output/appendix_figures/A_XIV_single_b','-depsc')

    figure;
    plot(nn,RPath_plot,'-o','LineWidth',6,'MarkerSize',10,'MarkerFaceColor','#0072BD')
        axis([0 25 1.01 1.07])
        set(gca,'FontSize',14)
        xline(N,':r',{'Graduation','Step N'},'LineWidth',2.5,'FontSize',14,'Color',[0.6350 0.0780 0.1840],'LabelVerticalAlignment', 'top');
        xlabel('Step n', 'FontSize',19)
    print('../../output/appendix_figures/A_XIV_single_c','-depsc')

    figure;
    plot(nn,DtPath_plot,'-o','LineWidth',6,'MarkerSize',10,'MarkerFaceColor','#0072BD')
        axis([0 25 0 6])
        set(gca,'FontSize',14)
        xline(N,':r',{'Graduation','Step N'},'LineWidth',2.5,'FontSize',14,'Color',[0.6350 0.0780 0.1840],'LabelVerticalAlignment', 'bottom');
        xlabel('Step n', 'FontSize',18)
    print('../../output/appendix_figures/A_XIV_single_d','-depsc')
    
end    