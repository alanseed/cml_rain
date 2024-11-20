#include <Eigen/Dense>
#include <iostream>
#include <cmath>
#include <vector>

struct Observations {
    double value;
    double x;
    double y;
};

class Kriging {
public:
    Eigen::MatrixXd buildGammaMatrix(const std::vector<Observations>& observations) {
        int n = observations.size();
        Eigen::MatrixXd gamma = Eigen::MatrixXd::Zero(n + 1, n + 1);

        for (int i = 0; i < n; ++i) {
            for (int j = 0; j <= i; ++j) {
                double dx = observations[i].x - observations[j].x;
                double dy = observations[i].y - observations[j].y;
                double dist = std::sqrt(dx * dx + dy * dy);
                gamma(i, j) = gamma(j, i) = variogram(dist);
            }
            gamma(i, n) = gamma(n, i) = 1.0;
        }

        gamma(n, n) = 0.0; // Lagrange multiplier
        return gamma;
    }

    Eigen::VectorXd solveWeights(const Eigen::MatrixXd& gamma, const Eigen::VectorXd& values) {
        Eigen::VectorXd rhs(values.size() + 1);
        rhs.head(values.size()) = values;
        rhs(values.size()) = 1.0; // Constraint for weights to sum to 1

        // Solve gamma * weights = rhs
        Eigen::VectorXd weights = gamma.ldlt().solve(rhs);
        return weights;
    }
    
    double variogram(double distance) {
        // Example spherical variogram model
        double range = 10000.0; // Range parameter
        double sill = 1.0;  // Sill parameter
        if (distance > range) return sill;
        return sill * (1.5 * distance / range - 0.5 * std::pow(distance / range, 3));
    }
private:

};
