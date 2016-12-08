

# libaries 
import numpy as np
import matplotlib.pyplot as pl
import pyGPs as gp
from math import log, pi, sqrt


class LoadModel:
    '''GP model that predict power load using data in DEBS 2014 challenges'''
    

    def __init__(self, X, Y, hyp = [], verbose = 1):

        self.verbose = verbose
        # training data
        self.gprX = np.array(X)
        self.gprY = np.array(Y).reshape(-1, 1)
        # initial hyperparameters
        if hyp == []:
            self.gp_hyp = [0.5]*(len(X[0])+1)
        else:
            self.gp_hyp = hyp


    def train_model(self):
        if self.verbose :
            print ("training GP model ...")

        self.gpMdl = gp.GPR()
        m = gp.mean.Zero()
        k = gp.cov.RBFard(D=None, log_ell_list=self.gp_hyp[:-1], log_sigma=self.gp_hyp[-1])



        '''
        try:
            self.gpMdl.setPrior(mean=m, kernel=k)
            self.gpMdl.setNoise(log_sigma = np.log(0.8))
            self.gpMdl.setOptimizer('BFGS')#('minimize');
            #self.gpMdl.getPosterior(self.gprX, self.gprY)
            self.gpMdl.optimize(self.gprX,self.gprY)#,numIters=100)
        except:

            print('cannot quasi-newton it')  '''
        #self.gpMdl = gp.GPR()
        self.gpMdl.setPrior(mean=m, kernel=k)
        self.gpMdl.setNoise(log_sigma = np.log(0.7))
        #self.gpMdl.getPosterior(self.gprX,self.gprY)
        self.gpMdl.setOptimizer('Minimize')
        self.gpMdl.optimize(self.gprX,self.gprY, numIterations=10)#,numIters=100)

        return self.gpMdl.covfunc.hyp


    def predict_load(self, Xs):

        if self.verbose:
            print ("predicting ...")
        # predict at Xs 
        Y_mean, Y_si, foom, foos2, foolp = self.gpMdl.predict(np.array(Xs).reshape(1, -1))
    
        if  Y_mean[0] < 0:   #bound the prediction not to get negative
            Y_mean[0] = 0
         
        return Y_mean[0], Y_si[0]
        
    

    def cal_Error_Metrics(self, y_m, y_si, y_real):
        MSE = 0
        MRSE = 0
        LD = 0
        n = len(y_real)
        Error = [y_m[i] - y_real[i] for i in range(n)]
        

        # Calculate Mean Square Error
        MSE = 1.0 / n * sum(np.power(Error, 2).tolist())

        # Calculate Mean Root Square Error
        MRSE = 1.0 * sqrt(sum(np.power(Error, 2).tolist()) / sum(np.power(y_real, 2).tolist()))

        # Calculate Log predictive density error
        LD = 0.5 * log(2 * pi) + 1 / (2 * n) * \
            sum(np.log(np.power(y_si[:n], 2)).tolist() + np.divide(np.power(Error, 2), np.power(y_si[:n], 2)).tolist())

        print('MSE: ' + str(MSE))
        print('MRSE: ' + str(MRSE))
        print('LD: ' + str(LD))

        return MSE,MRSE,LD

    def plot_results(self, y_m, y_si, y_real):
        # Draw the Results
        n = len(y_real)
        sigma = np.sqrt(y_si[:n])

        lowerBound = np.array(y_m[:n]) - 2 * sigma
        upperBound = np.array(y_m[:n]) + 2 * sigma
        for i in range(len(lowerBound)):
            if lowerBound[i] < 0:
                lowerBound[i] = 0

        fig, ax1 = pl.subplots(1, 1)

        # time

        x = range(n)
        #######  PLOT latency prediction (output)
        ax1.plot(y_real, 'b-', markersize=5, label=u'Observations')
        ax1.plot(y_m[:n], 'r--', label=u'Prediction')
        ax1.fill(np.concatenate([x, x[::-1]]),
                 np.concatenate([lowerBound, upperBound[::-1]]),
                 #       alpha=.5, fc='b', ec='b', label='95% confidence interval')
                 alpha=.75, fc='w', ec='k', label='95% confidence interval')
        ax1.set_xlabel('$time$')
        ax1.set_ylabel('$Latency$')
        ax1.legend(loc='upper right')
        ax1.set_xlim(0, n + 1)
        ax1.grid(True)
        pl.show()
