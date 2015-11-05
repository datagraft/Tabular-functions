(ns tabular-functions.pipeline
  (
  :require
     [grafter.tabular :refer :all]
     [grafter.rdf :refer [s]]
     [grafter.rdf.protocols :refer [->Quad]]
     [grafter.rdf.templater :refer [graph]]
     [grafter.vocabularies.rdf :refer :all]
     [grafter.vocabularies.foaf :refer :all]
     [incanter.core :as inc]  
     [tabular-functions.aggregations :refer :all]
     [tabular-functions.datatypes :refer :all]))
;(defn -main
;  "I don't do a whole lot ... yet."
;  [& args]
;  (println "Hello, World!"))

(defn add-row "Inserts new row into a dataset. Two options are available:
  1. Takes a dataset and vector containing field values and appends new row to the end of a dataset, e.g.
          
          Given original dataset
          
              |:col1|:col2|:col3|
              |-----|-----|-----|
              | 1   | 2   | 3   |
              | 4   | 5   | 6   |
              | 7   | 8   | 9   |

          function returns the following result:
       
              `(add-row dataset [10 11 12]) ; =>`

      
              |:col1|:col2|:col3|
              |-----|-----|-----|
              | 1   | 2   | 3   |
              | 4   | 5   | 6   |
              | 7   | 8   | 9   |
              | 10  | 11  | 12  | 

  2. Takes a dataset, row index and vector containing field values and inserts new row at the specified position. 
     If position index is negative or greater then total number of rows in a dataset, the new row will be apended to a dataset.

          Given original dataset
          
              |:col1|:col2|:col3|
              |-----|-----|-----|
              | 1   | 2   | 3   |
              | 4   | 5   | 6   |
              | 7   | 8   | 9   |

          function returns the following result:
       
              `(add-row dataset 1 [10 11 12]) ; =>`

      
              |:col1|:col2|:col3|
              |-----|-----|-----|
              | 1   | 2   | 3   |
              | 10  | 11  | 12  | 
              | 4   | 5   | 6   |
              | 7   | 8   | 9   |
              
              
              `(add-row dataset -2 [10 11 12]) ; =>`

      
              |:col1|:col2|:col3|
              |-----|-----|-----|
              | 10  | 11  | 12  | 
              | 1   | 2   | 3   |
              | 4   | 5   | 6   |
              | 7   | 8   | 9   |


    For both options if number of parameters denoting field values is less than current number of columns in a dataset, 
    lacking values for columns will remain empty. If number of parameters denoting field values is greater than number 
    of columns in a dataset, rest of the values will be discarded." 
 ( [dataset [& values]]
    (-> (make-dataset (:rows (incanter.core/conj-rows dataset values)) (column-names dataset))(with-meta (meta dataset))))
 ( [dataset position [& values]]
    ( if (or (< position 0) (>= position (count (:rows dataset)))) (add-row dataset [values])
    (-> (make-dataset (:rows (incanter.core/conj-rows (take-rows dataset position ) 
                                                      values 
                                                      (rows dataset (range position (count (:rows dataset))))
                                                      )) 
                      (column-names dataset))
        (with-meta (meta dataset))
        )))
)
   
(defn shift-row "Changes row's position inside a dataset. Two options are available:
  1. Takes a dataset and row index and moves this row to the end of a dataset, data rows with indices greater than specified index 
     will be moved one position up e.g.
          
          Given original dataset
          
              |:col1|:col2|:col3|
              |-----|-----|-----|
              | 1   | 2   | 3   |
              | 4   | 5   | 6   |
              | 7   | 8   | 9   |
              | 10  | 11  | 12  | 

          function returns the following result:
       
              `(shift-row dataset 1) ; =>`

      
              |:col1|:col2|:col3|
              |-----|-----|-----|
              | 1   | 2   | 3   |
              | 7   | 8   | 9   |
              | 10  | 11  | 12  | 
              | 4   | 5   | 6   |

  2. Takes a dataset and two row indices and moves row from index #1 to index #2.  
     Other rows will be shifted appropriately.

          Given original dataset
          
              |:col1|:col2|:col3|
              |-----|-----|-----|
              | 1   | 2   | 3   |
              | 4   | 5   | 6   |
              | 7   | 8   | 9   |
              | 10  | 11  | 12  | 

          function returns the following result:
       
              `(shift-row dataset 1 3) ; =>`

      
              |:col1|:col2|:col3|
              |-----|-----|-----|
              | 1   | 2   | 3   |
              | 7   | 8   | 9   |
              | 10  | 11  | 12  | 
              | 4   | 5   | 6   |
                
   " 
 ( [dataset position-from]
  ( -> (make-dataset (:rows (incanter.core/conj-rows (take-rows dataset position-from) 
                                                     (rows dataset (range (+ position-from 1) (count (:rows dataset))))
                                                     (rows dataset [position-from])))
                     (column-names dataset))
       (with-meta (meta dataset))

  )
 )
 ( [dataset position-from position-to]
  (let [f (+ position-from 1) 
        t (+ position-to 1) 
        eods (count (:rows dataset)) ] 
         (-> (make-dataset (cond (< position-from position-to) (
                                                  :rows (incanter.core/conj-rows 
                                                                        (take-rows dataset position-from)
                                                                        (rows dataset (range f t))
                                                                        (rows dataset [position-from])
                                                                        (rows dataset (range t eods))))

                                  :else (
                                                  :rows (incanter.core/conj-rows 
                                                                        (take-rows dataset position-to)
                                                                        (rows dataset [position-from])
                                                                        (rows dataset (range position-to position-from))
                                                                        (rows dataset (range f eods))
                                                                 
                                                              )))

                           (column-names dataset))
             (with-meta (meta dataset))
             )
         )
         )
  )
 
(defn sort-dataset "Sorts dataset by given column name(s) in given order. Type of comparator used for sorting is defined 
  by the third parameter as one of following:
                   - :alpha for alphabetical sorting;
                   - :num for numerical sorting;
                   - :len for sorting by field length;
                   - :date for sorting dates

  Order is given in forth parameter as either :asc for ascending and :desc for descending. 
  Sorting by multiple columns works as follows: if several rows have equal columns(first in the vector of given columns) 
  according to the given comparator type, these rows will be sorted by second column, if both first and second are equal, 
  sorting will be performed by the third column etc.                 
  
  Examples:
                   
             Given original dataset
   
                   |  :a |                 :b | :c |         :d |
                   |-----+--------------------+----+------------|
                   |   2 |             string |  1 | 01.01.2015 |
                   | 111 |             string |  3 | 03.11.2015 |
                   |  44 |      longer string |  9 | 03.03.2013 |
                   |   3 | the longest string |  6 | 25.12.2015 |


             calling function with different parameters results in following datasets:

                `(sort-dataset [:a] :alpha :asc) ; sort by column :a in ascending alphabetical order =>`
                   

                   |  :a |                 :b | :c |         :d |
                   |-----+--------------------+----+------------|
                   | 111 |             string |  3 | 03.11.2015 |
                   |   2 |             string |  1 | 01.01.2015 |
                   |   3 | the longest string |  6 | 25.12.2015 |
                   |  44 |      longer string |  9 | 03.03.2013 |



               `(sort-dataset [:a] :alpha :desc) ; sort by column :a in descending alphabetical order =>`

                   |  :a |                 :b | :c |         :d |
                   |-----+--------------------+----+------------|
                   | 111 |             string |  3 | 03.11.2015 |
                   |   2 |             string |  1 | 01.01.2015 |
                   |   3 | the longest string |  6 | 25.12.2015 |
                   |  44 |      longer string |  9 | 03.03.2013 |


               `(sort-dataset [:a] :num :asc) ; sort by column :a in ascending numerical order =>`

                   |  :a |                 :b | :c |         :d |
                   |-----+--------------------+----+------------|
                   |   2 |             string |  1 | 01.01.2015 |
                   |   3 | the longest string |  6 | 25.12.2015 |
                   |  44 |      longer string |  9 | 03.03.2013 |
                   | 111 |             string |  3 | 03.11.2015 |


               `(sort-dataset [:b] :len :asc) ; sort by column :b in ascending order by field length =>`


                   |  :a |                 :b | :c |         :d |
                   |-----+--------------------+----+------------|
                   |   2 |             string |  1 | 01.01.2015 |
                   |   3 | the longest string |  6 | 25.12.2015 |
                   |  44 |      longer string |  9 | 03.03.2013 |
                   | 111 |             string |  3 | 03.11.2015 |
                   
                   
               `(sort-dataset [:d] :date :asc) ; sort by column :d in ascending order by date =>`


                   |  :a |                 :b | :c |         :d |
                   |-----+--------------------+----+------------|
                   |  44 |      longer string |  9 | 03.03.2013 |
                   |   2 |             string |  1 | 01.01.2015 |
                   | 111 |             string |  3 | 03.11.2015 |
                   |   3 | the longest string |  6 | 25.12.2015 |


               `(sort-dataset [:b :a] :len :asc) ; sort by columns :b, :a in ascending order by field length =>`


                   |  :a |                 :b | :c |         :d |
                   |-----+--------------------+----+------------|
                   |   2 |             string |  1 | 01.01.2015 |
                   | 111 |             string |  3 | 03.11.2015 |
                   |  44 |      longer string |  9 | 03.03.2013 |
                   |   3 | the longest string |  6 | 25.12.2015 |


                   " [dataset colname sorttype mode] 
  
  (let [f (cond 
            (and (= sorttype :alpha) (= mode :asc))    #(compare (str %1) (str %2)) 
            (and (= sorttype :alpha) (= mode :desc))   #(compare (str %2) (str %1)) 
            (and (= sorttype :num) (= mode :asc))      #(<  (Double/parseDouble (str %1))  (Double/parseDouble (str %2)))  
            (and (= sorttype :num) (= mode :desc))     #(>  (Double/parseDouble (str %1))  (Double/parseDouble (str %2))) 
            (and (= sorttype :len) (= mode :asc))      #(< (count (str %1)) (count (str %2))) 
            (and (= sorttype :len) (= mode :desc))     #(> (count (str %1)) (count (str %2)))
            )] 
    
    (-> (make-dataset  (sort-by colname f (:rows dataset)) (column-names dataset)) (with-meta (meta dataset)))))

(defn shift-column "Changes column's position inside a dataset. Two options are available:
  1. Takes a dataset and column name/index and moves this column to the last position, data columns with indices greater than 
  specified index will be moved one position left e.g.
          
          Given original dataset
                
                | :a | :b | :c | :d |
                |----+----+----+----|
                |  1 |  2 |  3 |  a |
                |  4 |  5 |  6 |  b |
                |  7 |  8 |  9 |  c |


          function returns the following result:
            
          `(shift-column :b) ; =>`
          
          which is equivalent to
          `(shift-column 1) ; =>`

                | :a | :c | :d | :b |
                |----+----+----+----|
                |  1 |  3 |  a |  2 |
                |  4 |  6 |  b |  5 |
                |  7 |  9 |  c |  8 |


  2. Takes a dataset, column name/index and index where this column should be moved, moves given column to the specified index.  
  Other columns will be shifted appropriately.
          
          Given original dataset
                
                | :a | :b | :c | :d |
                |----+----+----+----|
                |  1 |  2 |  3 |  a |
                |  4 |  5 |  6 |  b |
                |  7 |  8 |  9 |  c |


          function returns the following result:
            
          `(shift-column :c 0) ; =>`
          
          which is equivalent to
          `(shift-column 2 0) ; =>`
                
                
                | :c | :a | :b | :d |
                |----+----+----+----|
                |  3 |  1 |  2 |  a |
                |  6 |  4 |  5 |  b |
                |  9 |  7 |  8 |  c |

"
 ([dataset column]
     (let [data (:rows dataset)
           header (column-names dataset)
           colname (if (keyword? column) column (get (column-names dataset) column))]

        (-> (make-dataset data (conj (into [] (remove #{colname} header )) colname))
                         (with-meta (meta dataset)))))
 
 ([dataset column position-to]
     (let [data (:rows dataset)
           header (column-names dataset)
           colname (if (keyword? column) column (get (column-names dataset) column))
           position-from (.indexOf header colname)
           last-pos (- (count header) 1)]

        (-> (make-dataset data (into []   (cond 
                                            (>= position-to last-pos)
                                              (shift-column colname)
                                            
                                            (< position-from position-to) 
                                              (concat
                                                (subvec header 0 position-from)
                                                (subvec header (+ position-from 1) (+ position-to 1))
                                                [colname]
                                                (subvec header (+ position-to 1)))
                                            
                                            (>= position-from last-pos)
                                              (concat
                                                (subvec header 0 position-to)
                                                [colname]
                                                (subvec header  position-to last-pos))
                                              
                                            :else
                                              (
                                               concat
                                                (subvec header 0 position-to)
                                                [colname]
                                                (subvec header position-to position-from)
                                                (subvec header (+ position-from 1)))
                                            )
                                    )
                                            
                                      ) 
                         (with-meta (meta dataset)))))
 )
 
 
(defn remove-columns "Removes columns from a dataset. Two options are available:
  1. Takes a dataset and vector of column names and creates a new dataset containing all columns except of those that were specified.

         
            Given original dataset
                  
                     | :col1 | :col2 | :col3 | :col4 |
                     |-------+-------+-------+-------|
                     |     1 |     2 |     3 |     4 |
                     |     5 |     6 |     7 |     8 |
                     |     9 |    10 |    11 |    12 |


          function returns the following result:
            
          `(remove-columns [:col1 :col4]) ; =>`

                     | :col2 | :col3 |
                     |-------+-------|
                     |     2 |     3 |
                     |     6 |     7 |
                     |    10 |    11 |


  2. Takes a dataset and two indices and creates a new dataset containing all columns execept of columns having indices within 
     the specified interval (including both points)                     

          Given original dataset

                     | a | b | c | d | e | f |
                     |---+---+---+---+---+---|
                     | 0 | 0 | 0 | 0 | 0 | 0 |
                     | 1 | 1 | 1 | 1 | 1 | 1 |
                     | 2 | 2 | 2 | 2 | 2 | 2 |


          function returns the following result:

          `(remove-columns 2 4) ; Remove columns having indices within the interval [2, 4] =>`

                     | a | b | f |
                     |---+---+---|
                     | 0 | 0 | 0 |
                     | 1 | 1 | 1 |
                     | 2 | 2 | 2 |

          " 
  ([dataset cols] (columns dataset (remove (fn [item] (some (fn [a] (= item a)) cols)) (column-names dataset))))
  ([dataset indexFrom indexTo] (cond 
                                 (= indexTo (count (column-names dataset))) 
                                    (columns dataset (range 0 indexFrom))  
                                 :else
                                    (columns dataset (concat 
                                                       (range 0 indexFrom)
                                                       (range (+ indexTo 1) (count (column-names dataset)))))))

  )

(defn merge-columns "Merges several columns in one using specified separator between columns. Two options are available:
  1. Takes a dataset, vector of columns and separator and merges columns together. Column containing the result of the merge 
  gets the same name as the first column in the list of arguments:
                    
          Given original dataset:
                    
                    | :name |   :city | :country |            :email |
                    |-------+---------+----------+-------------------|
                    | Alice |    Oslo |   Norway | alice@example.com |
                    |   Bob | Drammen |   Norway |   bob@example.com |

   

          function returns the following result:

          `(merge-columns [:city :country] \", \") ;  =>`

                    | :name |           :city |            :email |
                    |-------+-----------------+-------------------|
                    | Alice |    Oslo, Norway | alice@example.com |
                    |   Bob | Drammen, Norway |   bob@example.com |

                    
  2. Takes a dataset, vector of columns, separator and new column name and merges columns together.

          Given original dataset:
 
                    | :name |   :city | :country |            :email |
                    |-------+---------+----------+-------------------|
                    | Alice |    Oslo |   Norway | alice@example.com |
                    |   Bob | Drammen |   Norway |   bob@example.com |

   

          function returns the following result:

          `(merge-columns [:city :country] \", \" :place) ;  =>`

                    | :name |          :place |            :email |
                    |-------+-----------------+-------------------|
                    | Alice |    Oslo, Norway | alice@example.com |
                    |   Bob | Drammen, Norway |   bob@example.com |

                    "
  ([dataset columns separator]
  (let [pos (.indexOf (column-names dataset) (nth columns 0))
        [colon & colname] (str (nth columns 0))
        tempname (keyword (str (apply str colname) "_merged_temp"))] 
    (-> (derive-column dataset tempname columns (fn [& strings] (clojure.string/join separator strings)))
        (shift-column tempname pos)
        (remove-columns columns) 
        (rename-columns {tempname (keyword (str (apply str colname)))}))
    ))
  ([dataset columns separator newname]
  (let [pos (.indexOf (column-names dataset) (nth columns 0))]
    (-> (derive-column dataset newname columns (fn [& strings] (clojure.string/join separator strings)))
        (shift-column newname pos)
        (remove-columns columns))
    ))
  )

(defn remove-duplicates "Removes duplicates from a dataset. Two options are available:
  
  1. Given a dataset sorts it and looks for rows having the same values across all columns and leaves only one instance 
  from each set of such rows, other rows(duplicates) will be removed from a dataset.
                        
        Given original dataset:
                    
                    | :name |    :age |  :gender |
                    |-------+---------+----------|
                    | Alice |      18 |   female |
                    |   Bob |      30 |     male |
                    | Alice |      28 |   female |
                    | Alice |      18 |   female |
                    |   Bob |      32 |     male |

   

          function returns the following result:

          `(remove-duplicates) ;  =>`

                    | :name |    :age |  :gender |
                    |-------+---------+----------|
                    | Alice |      18 |   female |
                    | Alice |      28 |   female |
                    |   Bob |      30 |     male |
                    |   Bob |      32 |     male |
                        
  2. Given a dataset and a column(sequence of columns) looks for rows having the same values in the specified field(s) and leaves 
  only the first encountered row in this sequence. Dataset should be sorted in desired order before function is called
                    
                        
        Given original dataset:
                    
                    | :name |    :age |  :gender |
                    |-------+---------+----------|
                    | Alice |      18 |   female |
                    |   Bob |      30 |     male |
                    | Alice |      28 |   female |
                    |   Bob |      32 |     male |

   

        function returns the following result:

          `(->(sort-dataset [:name :age] :alpha :desc) (remove-duplicates [:name :gender]) ;  Dataset is first sorted in a such way 
                                                                                              that the records about the same person 
                                                                                              are given in descending order by age =>`

                    | :name |    :age |  :gender |
                    |-------+---------+----------|
                    | Alice |      28 |   female |
                    |   Bob |      32 |     male |
                        "
  ([dataset])
  ([dataset colnames])
  ([dataset colnames colnames-separators]))


(defn raise "Given a dataset, name of variable-column that should be used to generate new columns(raise from row values to column names) and
  name of a value-column containing the values that should be used to fill the new column reshapes a dataset. Function performs 
  the opposite actions  compared to the Grafter `melt` function. All columns except of value-column act as unique id, if duplicates 
  are found, function will throw an error. Example:
            
               Given original dataset:

                    |      :company-name |             :position |  :total-employed |
                    |--------------------+-----------------------+------------------|
                    |              Cisco | Jr.Software developer |               22 |
                    |              Cisco | Sr.Software developer |               10 |
                    |              Cisco |                Intern |                2 |
                    | Oracle corporation |        Assist.manager |                2 |
                    | Oracle corporation | Sr.Software developer |               38 |
                    |                IBM |        Assist.manager |                2 |
                    |                IBM | Jr.Software developer |                8 |
                    |              Cisco |        Assist.manager |                3 |
                    |                IBM | Sr.Software developer |                5 |
                    |                IBM |                Intern |                4 |


               function returns the following result:

               `(raise :position :total-employed) ;  =>`


                    |      :company-name | :Jr.Software developer | :Sr.Software developer | :Intern | :Assist.manager |
                    |--------------------+------------------------+------------------------+---------+-----------------|
                    |              Cisco |                     22 |                     10 |       2 |               3 |  
                    |                IBM |                      8 |                      5 |       4 |               2 |
                    | Oracle corporation |                        |                     38 |         |               2 |

            "
  [dataset variable value]
  (
   ;sort
   ;group by all but mentioned columns
   ;
   )
  )


(defn group-rows "Given a dataset, vector of column names and map of colnames--functions and creates a new dataset containg rows grouped by
  colnames from vector and the result of applying functions to correspondent column values. Each function in a map should 
  take collection as  a parameter and return a single value. 
               
                 For most common aggregations there exists set of pre-defined functions:
                 - MIN
                 - MAX
                 - SUM
                 - AVG
                 - COUNT


        Given original dataset:
                    
                    | :name |    :order | :payed |
                    |-------+-----------+--------|
                    | Alice |      1111 |    110 |
                    |   Bob |       998 |     55 |
                    | Alice |      1112 |     35 |
                    | Alice |      1113 |     25 |
                    |   Bob |       999 |    375 |

   

          function returns the following result:

          `(group-rows [:name] {:payed SUM}) ;  =>`

                    | :name | :payed_SUM |
                    |-------+------------|
                    | Alice |        170 |
                    |   Bob |        430 |
                 "
  [dataset colnames colnames-functions ])


(defn join-dataset "Joins two datasets together. Two options are available:
                   
  1. Takes a dataset, filename and type of concatenation (either :v to concatenate datasets vertically -- append data from file 
  to the right side of given dataset or :h to concatenate datasets horizontally -- append data from file to the bottom of 
  given dataset). 
  
  Throws an error if number of columns/rows is not appropriate
                   
  Examples.

        Given original dataset:
                    
                    | :name |    :age |  :gender |
                    |-------+---------+----------|
                    | Alice |      18 |   female |
                    |   Bob |      30 |     male |
        
        file 'left-part.csv' with content:
                   
                   email,country
                   alice@example.com,Norway
                   bob@example.com,Norway
        

          function returns the following result:

          `(join-dataset \"left-part.csv\" :v) ;  =>`

                    | :name |    :age |  :gender |            :email | country | 
                    |-------+---------+----------+-------------------+---------|
                    | Alice |      18 |   female | alice@example.com |  Norway |
                    |   Bob |      30 |     male |   bob@example.com |  Norway |

       
        Given the same original dataset and file 'other-persons.csv' with content:
                   
                   name,age,gender
                   John,38,male
                   Mary,27,female

          function returns the following result:

          `(join-dataset \"other-persons.csv\" :h) ;  =>`
                    
                    | :name |    :age |  :gender |
                    |-------+---------+----------|
                    | Alice |      18 |   female |
                    |   Bob |      30 |     male |
                    |  John |      38 |     male |
                    |  Mary |      27 |   female |
        
              
  2. Takes a dataset, filename, column that acts as foreign key in original dataset and columns for id and value in file. 
  Builds a lookup table from file and maps values in original dataset appropriately.
                   
        Given original dataset:
                    
                    | :name |    :age |  :position |
                    |-------+---------+------------|
                    | Alice |      18 |         25 |
                    |   Bob |      30 |          7 |
        
        file 'position-codes.csv' with content:
                   
                   code,description
                   1,manager
                   7,engineer
                   8,senior engineer
                   25,accountant
        

          function returns the following result:

          `(join-dataset \"position-codes.csv\" :position \"code\" \"description\") ;  =>`


                    | :name |    :age |  :position |
                    |-------+---------+------------|
                    | Alice |      18 | accountant |
                    |   Bob |      30 |   engineer |
                   "
  ([dataset filename concat-type])
  ([dataset filename fkey id value] )
  )

(defn split-column "Given a dataset, column name and separator splits specified column into multiple by separator. New columns get 
 names of a form [original-column-name]_splitted_0, [original-column-name]_splitted_1, ...

                   
          Given original dataset:

                    | :name |                       :address |            :email |
                    |-------+--------------------------------+-------------------|
                    | Alice | New York, Harrison Street, 507 | alice@example.com |
                    |   Bob |     Richmond, Main Street, 17  |   bob@example.com |

   

          function returns the following result:

          `(split-column :address \", \") ;  =>`

                    | :name | address_splitted_0 |  address_splitted_1 | address_splitted_2 |            :email |
                    |-------+--------------------+---------------------+--------------------+-------------------|
                    | Alice |           New York |     Harrison Street |                507 | alice@example.com |
                    |   Bob |           Richmond |         Main Street |                17  |   bob@example.com |
 " 
  [dataset colname separator]
  ( derive-column dataset :new [colname] (fn [col] (clojure.string/split col separator)))
  
)
