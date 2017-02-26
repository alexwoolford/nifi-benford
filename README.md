# Nifi Benford

This is a Nifi processor that uses [Benford's Law](https://en.wikipedia.org/wiki/Benford's_law) to categorize input into:
* conforming
* non-conforming
* insufficient sample size

The Chi-squared test is used to compare the distribution of values of the first digit to the expected distribution based on Benford's Law.

## to deploy on Nifi

Build the processor .nar file (presumably `.nar` stands for Nifi ARchive):

    nifi-benford
    mvn clean package

Then copy the .nar file in `nifi-benford/nifi-nifibenford-nar/target/nifi-nifibenford-nar-1.0.nar` to the Nifi lib folder (which, in my case is `/usr/hdf/2.1.0.0-165/nifi/lib/`), and restart Nifi.


[![Benford's Law Nifi processor](https://img.youtube.com/vi/xIGybxBmKpk/0.jpg)](https://www.youtube.com/watch?v=xIGybxBmKpk)
