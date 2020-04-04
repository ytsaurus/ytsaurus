#!/usr/bin/env python

import logging
import argparse

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.svm import l1_min_c

"""
Learn fitness coefficient given training sets.
"""


def learn(target, features):
    cs = l1_min_c(features, target, loss="log") * np.logspace(0, 4)
    clf = LogisticRegression(penalty="l1", tol=1e-6)

    for c in cs:
        clf.set_params(C=c)
        clf.fit(features, target)
        coefs = clf.coef_.ravel()
        sparsity = np.mean(coefs == 0)
        print "C=%-8.2f Sparsity = %-6.1f Score=%-6.4f  %s" % (
            c, 100.0 * sparsity, clf.score(features, target), str(coefs).replace("\n", " "))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("train_set", type=str, nargs="+", help="Training set to use")
    parser.add_argument("--silent", action="store_true", help="Do not log anything")
    parser.set_defaults(train_set=[])

    args = parser.parse_args()

    if args.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    target = []
    features = []
    for train_set in args.train_set:
        with open(train_set, "r") as handle:
            for line in handle:
                coefs = map(float, line.split())
                klass, coefs = int(coefs[0]), coefs[1:]
                assert klass in [0, 1]
                target.append(klass)
                features.append(coefs)

    target = np.array(target)
    features = np.array(features)

    logging.info("Loaded training data (target=%s, features=%s)", target.shape, features.shape)

    learn(target, features)
