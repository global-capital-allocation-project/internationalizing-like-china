% Calculates the terminal belief pi_T for a path M_t and graduation date T

function piT = piTerminal(t,Mpath,epsW,epsC)


pi0 = epsW;
for iter = 1:t
    pi1 = epsW + (1-epsC -epsW)/Mpath(iter)*pi0;
    pi0=pi1;
end
piT = pi1;

end