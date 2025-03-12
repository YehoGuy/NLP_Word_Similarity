# NLP_Word_Similarity - By Guy Yehoshua

### Understanding the Input Dataset's Structure - Google's English All Biarcs dataset.

Items in the “biarcs” dataset reflect higher-order dependency relations that involve two arcs -- three connected content words.  
The biarcs datasets allows modeling information involving interactions between two modifiers of the same head, e.g. subject and object of the same verb (e.g (boy, ate, cookies)), as well as complex arguments of a head, e.g. adjectivial modifier of a verb’s argument (e.g ((small, boy), ate)).
By abstracting over the middle element, we could also get second-order information, e.g. (boy, *, cookies) and ((small, *), ate).

By further abstracting over the words, one may uncover second-order syntactic phenomena: maybe some verbs are more likely to have adjectives to their subjects than others?

**Example**:
experience      that/IN/compl/3 patients/NNS/nsubj/3 experience/VB/ccomp/0      3092

We can break this down (schematically) into:

- **“experience”** (the main word or one of the dependents)
- **“that/IN/compl/3”** (word + POS + dependency relation + index in the sentence)
- **“patients/NNS/nsubj/3”** (word + POS + dependency relation + index)
- **“experience/VB/ccomp/0”** (the main word/central verb + POS + relation + index)
- **3092** (a count—how many times this structure appears)

### Understanding The Article "Comparing Measures of Semantic Similarity"
1.2 -- 
Short Explanation of F and L in Section 1.2

In that section of the article, F and L are simply two different random variables:

L = the “lexeme” or “target word”
F = the “feature” (context word)

Whenever you see something like P(F = f | L = l), it means “the probability of feature f given that our target lexeme is l.”

## Data Preperation - Hadoop Map-Reduce on AWS EMR
### Step 1 - calculating count(F=f,L=l), count(F=f), count(L=l) for every f,l in the corpus.
### Step 2 - calculating count(F), count(L) using keys-containing-*s from Step 1.
!!!! Filtering only what is in the gold standard
!!!! ouputing the data in weka's ARFF format
### Step 3 - calculating the 24-D final vectors

## Training a ML classifier on Weka


### Guy Yehoshua, Software Engineering B.Sc.