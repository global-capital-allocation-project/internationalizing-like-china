% A bisection algorithm to find the zero of a decreasing function

function xsol = bisection_algorithm(fcn,xmin,xmax,maxIter)

for iter = 1:maxIter
    xguess = (xmin+xmax)/2;
    adjMin = (fcn(xguess)>0);

    xmin1 = adjMin.*xguess + ~adjMin.*xmin;
    xmax1 = ~adjMin.*xguess + adjMin.*xmax;

    xmin = xmin1;
    xmax = xmax1;
end
xsol = xguess;

end