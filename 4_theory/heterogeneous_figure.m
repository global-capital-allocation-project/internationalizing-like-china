%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internationalizing Like China


% Code to Produce Figure 6


% Feb 2024
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

clear all
close all
clc

%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Importing and Defining Parameters
%%%%%%%%%%%%%%%%%%%%%%%%%%%%

load('parameters/params1.mat') % Import parameters for the model

% Parameter definitions analogous to those in paper

ns = hs / (gam - (1-hs)); % n^s, net worth multiplier for stable, no control
nf = hf/(gam-(1-hf)); % n^f, net worth multiplier for flighty, no control

gs = (gam-(1-hs))/(gam-(1-hs)/(1-taubar)); % g^s
gf = (gam-(1-hf))/(gam-(1-hf)/(1-taubar)); % g^f

rhos = 1/beta*(gs-1)/gs; %rho^s 
rhof = 1/beta*(gf-1)/gf; %rho^f


Mscr = @(M) 1 - (1-M)*taubar; % Denominator of Interest Rate Schedule


%%% Policy and Value Functions for Committed Type %%%

D0s = @(M) 1/b0*(gam*Q*Mscr(M)-Rbar) ./ omeg(M); % Debt from stable investors
R0D = @(M) 1/2*Rbar./Mscr(M) + 1/2*gam*Q; % Interest rate schedule


% Value if only borrow from stable investors
I = @(M) A0 + D0s(M);                                               % Date 0 investment if only stable investors are let in
V0p = @(M) ns*(gam*Q*I(M)-R0D(M).*D0s(M));           % Value if only stable investors let in


% Value if borrow from flighty investors
D0f = @(M) 2*D0s(M); % Total debt
Ia = @(M) A0 + D0f(M); % Date 0 investment
V0a = @(M) nf*(gam*Q*Ia(M)-R0D(M).*D0f(M)); % Value if flighty investors let in


%% Check that net worth multiplier and debt levels are positive %%
gamCheck = gam-(1-hf)/(R1-taubar);
int_check=R1-taubar;

if gamCheck<0 && int_check<0
    disp('There is at least one parameter restriction that is not satisfied.')
    STOP
end


if Mscr(epsO) * gam * Q - Rbar < 0
    disp('Debt Level Negative')
    STOP
end


%%% Numerically Compute Open Up Threshold M* %%%
maxIter = 50;
Mstar = bisection_algorithm(@(M) V0p(M)-V0a(M),epsO,1-epsC,1000); % Bisection Algorithm to find M*

VOpen = V0p(Mstar); % Open up threshold in indirect utility





%%%%%% Algorithm for Solving Reputational Equilbirium %%%%


Nstars = 3; % Candidate open up step.

% Lowest and highest values of M when only borrow from stable investors
M0l = ones(1,length(Nstars))*epsO;
M0u = ones(1,length(Nstars))*Mstar;
% Indirect utility at these values
V0pl = V0p(M0l); 
V0pu = V0p(M0u);

VU = V0a(1-epsC); %This is the graduation threshold utility
Vastl = gs / gf * (1-rhos.^(Nstars+1))/(1-rhos) .* V0pl; % Lower flow value at opening up step
Vastu = gs / gf * (1-rhos.^(Nstars+1))/(1-rhos) .* V0pu; % Upper flow value at opening up step



% Given open up step, find the graduation step N = Nstar + s by iterating
% reputation building forward
V0newl = Vastl;
V0newu = Vastu;

hasGraduatedl = zeros(1,length(V0pl)); % record if graduation step found
hasGraduatedu = zeros(1,length(V0pl)); % record if graduation step found
graduationl = -1 * ones(1,length(V0pl)); % Record graduation step. Record -1 as a catch
graduationu = -1 * ones(1,length(V0pl)); % Record graduation step. Record -1 as a catch

for s = 0:500
    % Next period flow value. Both investors transition dynamics.
    V0newl = V0newl*rhof + gs/gf*V0pl;
    V0newu = V0newu*rhof + gs/gf*V0pu;
    
    % See whether graduate this period, if have not already graduated
    graduatesl = (1-hasGraduatedl).*(V0newl>VU);
    graduatesu = (1-hasGraduatedu).*(V0newu>VU);
    
    graduationl = graduatesl*s + (1-graduatesl).*graduationl;
    graduationu = graduatesu*s + (1-graduatesu).*graduationu;
    
    hasGraduatedl = hasGraduatedl + graduatesl;
    hasGraduatedu = hasGraduatedu + graduatesu;
end


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% We have found above candidate paths with starting M0, opening up step N*
% and graduation step N. Now we need to inspect which of these patterns, if
% any, is an equilibirum

foundSolution = 0;
openupii = 0;
Solution_number=0;
solTol = 1e-8;

% loop over open up steps
for openupii = 1:length(Nstars)
    Nstar = Nstars(openupii); % Open up step for this iteration

    % range of possible graduation steps for this iteration
    graduationl_ = graduationl(openupii);
    graduationu_ = graduationu(openupii);
    
    %this loop skips if graduation is set as -1 as a placeholder since then
    %there isn't a valid step. If either one is above (weakly) zero, then it
    %sets both to be weakly greater than zero
    if max(graduationl_,graduationu_)<0
        continue
    else
        graduationl_ = max(graduationl_,0);
        graduationu_ = max(graduationu_,0);
    end
    
    % Feasible graduation steps associated with opening up at N=openupii
    Ns = graduationu_:graduationl_;
    
    % Upper Bound on Graduation M0s: below VU at s=Ns (don't graduate at Ns-1)
    M0s_upper = bisection_algorithm(@(M) VU - gs/gf*(1/(1-rhof) + rhof.^(Ns)*((1-rhos^(Nstar+1))/(1-rhos)-1/(1-rhof))).*V0p(M),M0l(openupii)*ones(1,length(Ns)),M0u(openupii)*ones(1,length(Ns)),maxIter);

    % Lower Bound on Graduation M0s: above VU at s=Ns+1 (graduate at Ns)
    M0s_lower = bisection_algorithm(@(M) VU - gs/gf*(1/(1-rhof) + rhof.^(Ns+1)*((1-rhos^(Nstar+1))/(1-rhos)-1/(1-rhof))).*V0p(M),M0l(openupii)*ones(1,length(Ns)),M0u(openupii)*ones(1,length(Ns)),maxIter);
    
    M0min = M0s_lower;
    M0max = M0s_upper;
    int_size=M0min-M0max;

    % Defines the graduation steps in terms of M0 intervals


    M0s = [];
    terminal_gfps = [];
    unique_check = [];

    % Now, loop through graduation steps
    for ii = 1:length(Ns)
        n = Ns(ii);
        M0l = M0min(ii);
        M0u = M0max(ii);
        
        
        % Reputation path before opening up
        closedDates = [0:Nstar-1]; % steps before opening up
        
        
        Vpath_closed = @(m0) (1-rhos.^(closedDates+1))/(1-rhos)*V0p(m0); % solving the path forward
        
        % Invert indirect utility back into reputation
        if Nstar==1
            Mpath_closed = @(m0) m0;
        elseif Nstar>1
            Mpath_closed = @(m0) bisection_algorithm(@(M) Vpath_closed(m0) - V0p(M),ones(1,Nstar)*epsO,ones(1,Nstar)*Mstar,maxIter); 
        end
        
        % Indirect utility and reputation paths after opening up
        Vpath_open = @(m0) gs/gf*(1/(1-rhof) + rhof.^(0:n)*((1-rhos^(Nstar+1))/(1-rhos)-1/(1-rhof)))*V0p(m0);
        Mpath_open = @(m0) bisection_algorithm(@(M) Vpath_open(m0) - V0a(M),ones(1,n+1)*Mstar,ones(1,n+1)*(1-epsC),maxIter); % Given the above path of V0,t this inverts at each point in time to find the value Mt. Note that we know the function to be invertible.
        
        
        % Combine the two paths
        Vpath = @(m0) [Vpath_closed(m0),Vpath_open(m0)];
        Mpath = @(m0) [Mpath_closed(m0),Mpath_open(m0)];
        
        
        % Given the paths for V and M, we build the path for pi and the terminal
        % condition at graduation
        piN = @(m0) piTerminal(Nstar+n,Mpath(m0),epsO,epsC); % path of beliefs
        VN = @(m0) gs/gf*(1/(1-rhof) + rhof^(n)*((1-rhos^(Nstar+1))/(1-rhos)-1/(1-rhof)))*V0p(m0); % terminal indirect utility
        MN = @(m0) bisection_algorithm(@(M) VN(m0) - V0a(M),Mstar,1-epsC,maxIter); % terminal reputation
        
        M0_out = bisection_algorithm(@(m0) piN(m0) - MN(m0),M0l,M0u,maxIter); % find initial M0 as value such that all opportunistic types graduate at T
        
        
        M0s = [M0s,M0_out]; % stores candidate solutions
        terminal_gfps = [terminal_gfps,piN(M0_out) - MN(M0_out)]; % error in terminal condition piT = MT
        
    end
[~,loc] = min(abs(terminal_gfps)); % Finds the smallest possible error and returns its location in the vector

    if abs(terminal_gfps(loc)) < solTol
        disp('Solution Found')
        foundSolution = 1;
        Solution_number=Solution_number+1;
        
        M0 = M0s(loc);          % Sets the equilibrium M0
        N = Ns(loc);            % Sets the equilibrium N (counting from after Nstar)
        terminal_gfps(loc);     % Gap for the equilibrium terminal condition
        fprintf('Solution Number: %d \n',Solution_number);
        fprintf('Terminal Conditon Numerical Error: %d \n',terminal_gfps(loc));
        fprintf('Opening-Up Date: %d \n',Nstar)
        fprintf('Graduation Date: %d \n',Nstar+N)
        

        % Provide the path of reputation and utility for the equilibrium
        % (same code as above)
        closedDates = 0:Nstar-1;
        Vpath_closed = (1-rhos.^(closedDates+1))/(1-rhos)*V0p(M0);
            
            if Nstar==1
                Mpath_closed = M0;
            elseif Nstar>1
                Mpath_closed = bisection_algorithm(@(M) Vpath_closed - V0p(M),ones(1,Nstar)*epsO,ones(1,Nstar)*Mstar,maxIter); 
            end

        Vpath_open = gs/gf*(1/(1-rhof) + rhof.^(0:N)*((1-rhos^(Nstar+1))/(1-rhos)-1/(1-rhof)))*V0p(M0);
        Mpath_open = bisection_algorithm(@(M) Vpath_open - V0a(M),ones(1,N+1)*Mstar,ones(1,N+1)*(1-epsC),maxIter);
        Vpath = [Vpath_closed,Vpath_open];
        Vpath_plot= [Vpath,ones(1,10)*VU];                                                  % Adds to the above a few steps after graduation

        Mpath = [Mpath_closed,Mpath_open];                                                  % Computes the equilibrium path of M0
        Mpath_plot=[Mpath, ones(1,10)*(1-epsC)];                                            % Adds to the above a few steps after graduation
         

        % Verify that the bisection algorithm has not found an invalid
        % corner solution
if Mpath(Nstar+1)-Mstar<1e-8
    disp('Invalid Solution')
    continue
end


        piPath = piPathfcn(Nstar+N,Mpath,epsO,epsC);                                         % Computes the equilibrium path of beliefs of facing the committed type
        piPath_plot=[piPath,ones(1,10)*(1-epsC)];                                           % Adds to the above a few steps after graduation
        
        mpath = (Mpath-piPath)./(1-piPath);                                                 % Computes the equilibrium path of mimicking probability
        mpath_plot=[mpath,zeros(1,10)];                                                     % Adds to the above a few steps after graduation
        
        % Debt policies
        D0fPath_closed=D0s(Mpath_closed);
        D0fPath_open=D0f(Mpath_open);
        D0fPath=[D0fPath_closed, D0fPath_open];
        D0fPath_plot=[D0fPath, D0f(ones(1,10)*(1-epsC))];
        
        % Interest rate policies
        RPath_closed = R0D(Mpath_closed);
        RPath_open = R0D(Mpath_open);
        RPath = [RPath_closed, RPath_open];
        RPath_plot=[RPath, R0D(1-epsC)*ones(1,10)];

        
        % Date Range
        nn = [0:Nstar+N+10];



%%% This code plots separately the four panels of Figure 6 %%%


        figure;
        hold on
        plot(nn,Mpath_plot,'-o','LineWidth',6,'MarkerSize',10,'MarkerFaceColor','#0072BD')
        plot(nn,piPath_plot,':o','LineWidth',6,'MarkerSize',10,'MarkerFaceColor','#D95319')
        hold off
        axis([0 22 0 1.15])
        set(gca,'FontSize',14)
        xline(Nstar+N,':r',{'Graduation','Step N'},'LineWidth',2.5,'FontSize',14,'Color',[0.6350 0.0780 0.1840],'LabelVerticalAlignment', 'bottom');
        xline(Nstar,':',{'Open-Up','Step N^*'},'LineWidth',2.5,'FontSize',14,'Color',1/255*[0,104,87],'LabelVerticalAlignment', 'bottom');
        yline(1-epsC,':k',{'Max','Reputation'},'LineWidth',2.5,'FontSize',14,'LabelHorizontalAlignment', 'left');
        yline(Mstar,':',{'Open-Up Reputation: M^\ast'},'Color',[0.4940, 0.1840, 0.5560],'LineWidth',2.5,'FontSize',14)
        xlabel('Step n', 'FontSize',18)
        legend({'M_{n}','\pi_n'}, 'Location','southeast','FontSize',14)
        legend boxoff
        box on
        xticks([0 5 10 15 20])
        yticks([0 0.2 0.4 0.6 0.8 1])
        print('../../output/paper_figures/6_multiple_a.eps','-depsc')
        
        figure;
        plot(nn,mpath_plot,'-o','LineWidth',6,'MarkerSize',10,'MarkerFaceColor','#0072BD')
        axis([0 22 -0.05 1])
        set(gca,'FontSize',14)
        xline(Nstar+N,':r',{'Graduation','Step N'},'LineWidth',2.5,'FontSize',14,'Color',[0.6350 0.0780 0.1840],'LabelVerticalAlignment', 'top');
        xline(Nstar,':',{'Open-Up','Step N^*'},'LineWidth',2.5,'FontSize',14,'Color',1/255*[0,104,87],'LabelVerticalAlignment', 'bottom');
        xlabel('Step n', 'FontSize',18)
        xticks([0 5 10 15 20])
        yticks([0:0.1:1])
        print('../../output/paper_figures/6_multiple_b.eps','-depsc')

        figure;
        plot(nn,RPath_plot,'-o','LineWidth',6,'MarkerSize',10,'MarkerFaceColor','#0072BD')
        axis([0 22 1.02 1.08])
        set(gca,'FontSize',14)
        xline(Nstar+N,':r',{'Graduation','Step N'},'LineWidth',2.5,'FontSize',14,'Color',[0.6350 0.0780 0.1840],'LabelVerticalAlignment', 'top');
        xline(Nstar,':',{'Open-Up','Step N^*'},'LineWidth',2.5,'FontSize',14,'Color',1/255*[0,104,87],'LabelVerticalAlignment', 'top');
        xlabel('Step n', 'FontSize',19)
        xticks([0 5 10 15 20])
        yticks([1.02:0.01:1.08])
        print('../../output/paper_figures/6_multiple_c.eps','-depsc')

        figure;
        plot(nn,D0fPath_plot,'-o','LineWidth',6,'MarkerSize',10,'MarkerFaceColor','#0072BD')
        axis([0 22 0 5])
        set(gca,'FontSize',14)
        xline(Nstar+N,':r',{'Graduation','Step N'},'LineWidth',2.5,'FontSize',14,'Color',[0.6350 0.0780 0.1840],'LabelVerticalAlignment', 'bottom');
        xline(Nstar,':',{'Open-Up','Step N^*'},'LineWidth',2.5,'FontSize',14,'Color',1/255*[0,104,87],'LabelVerticalAlignment', 'bottom');
        xlabel('Step n', 'FontSize',18)
        xticks([0 5 10 15 20])
        yticks([0:0.5:5])
        print('../../output/paper_figures/6_multiple_d.eps','-depsc')

end

end