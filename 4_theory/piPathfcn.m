% Calculates the path of beliefs pi_t for a path M_t and graduation date T

function piPathOut = piPathfcn(T,Mpath,epsW,epsC)


piPathOut = zeros(1,T+1);
piPathOut(1) = epsW;

for iter = 1:T
    piPathOut(iter+1) = epsW + (1-epsC -epsW)/Mpath(iter)*piPathOut(iter);
end

end