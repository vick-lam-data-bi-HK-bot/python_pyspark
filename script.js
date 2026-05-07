const quizContainer = document.getElementById('quiz');
const sectionFilter = document.getElementById('sectionFilter');
const resetButton = document.getElementById('resetButton');

const questions = [
  {
    id: 1,
    section: 'Architecture & Core Concepts',
    question: 'What is the main reason a Spark shuffle causes high overhead?',
    options: [
      'It uses only in-memory data movement and avoids disk writes.',
      'It requires writing intermediate data to disk and transferring it over the network.',
      'It only occurs during narrow transformations like map and filter.',
      'It is used to cache RDD partitions in memory permanently.'
    ],
    answerIndex: 1,
    explanation: 'A shuffle redistributes data across partitions and typically writes intermediate data to disk, then transfers it over the network, causing I/O overhead and latency.'
  },
  {
    id: 2,
    section: 'Architecture & Core Concepts',
    question: 'What does Spark do before executing work for a DataFrame transformation sequence?',
    options: [
      'Executes each transformation immediately and stores the result on disk.',
      'Builds a logical plan and waits for an action before executing.',
      'Converts every DataFrame to an RDD first.',
      'Runs all optimizations after the first action is completed.'
    ],
    answerIndex: 1,
    explanation: 'Spark uses lazy evaluation: it builds a logical plan (DAG) and waits for an action so the Catalyst Optimizer can combine operations and skip unnecessary reads.'
  },
  {
    id: 3,
    section: 'Architecture & Core Concepts',
    question: 'Which deployment mode is usually preferred in production to keep the Driver close to the cluster?',
    options: [
      'local mode',
      'client mode',
      'cluster mode',
      'standalone mode'
    ],
    answerIndex: 2,
    explanation: 'In cluster mode, the Driver runs on a worker node inside the cluster, avoiding network bottlenecks between the submitting client and the cluster.'
  },
  {
    id: 4,
    section: 'Architecture & Core Concepts',
    question: 'What is the main difference between cache() and persist() in Spark?',
    options: [
      'cache() allows custom storage levels, persist() always uses MEMORY_ONLY.',
      'cache() uses MEMORY_AND_DISK by default while persist() can use custom storage levels.',
      'persist() always writes to HDFS and cache() does not.',
      'cache() is only available in Spark Streaming.'
    ],
    answerIndex: 1,
    explanation: 'cache() uses MEMORY_AND_DISK by default, while persist() lets you choose a storage level such as DISK_ONLY or MEMORY_ONLY.'
  },
  {
    id: 5,
    section: 'Architecture & Core Concepts',
    question: 'Why is Kryo serialization preferred over Java serialization in Spark?',
    options: [
      'Kryo is slower but more readable.',
      'Kryo is faster and more compact, reducing shuffle footprint.',
      'Java serialization uses less memory than Kryo.',
      'Kryo cannot serialize custom case classes.'
    ],
    answerIndex: 1,
    explanation: 'Kryo serialization is faster and more compact than Java serialization, which helps reduce the amount of data shuffled across the network.'
  },
  {
    id: 6,
    section: 'Data Ingestion & Source Control',
    question: 'Why should you avoid inferSchema=True on large input files?',
    options: [
      'It can only infer schema for CSV files, not Parquet.',
      'It requires an extra pass over the data, which is slow on large files.',
      'It automatically enables schema evolution and causes errors.',
      'It stores schema in the Spark UI only, not in the DataFrame.'
    ],
    answerIndex: 1,
    explanation: 'inferSchema=True forces Spark to read the data once to infer types. On large files, it is slower than defining an explicit StructType schema.'
  },
  {
    id: 7,
    section: 'Data Ingestion & Source Control',
    question: 'What does watermarking help prevent in streaming pipelines?',
    options: [
      'Data loss during checkpointing.',
      'Infinite growth of state for late-arriving event-time data.',
      'Sharding of partitioned tables in Delta Lake.',
      'Automatic schema inference for raw input.'
    ],
    answerIndex: 1,
    explanation: 'Watermarking tracks event time and can drop state for data arriving later than a threshold, preventing the state store from growing indefinitely.'
  },
  {
    id: 8,
    section: 'Data Ingestion & Source Control',
    question: 'Which file format is best for OLAP queries selecting a few columns?',
    options: [
      'JSON',
      'Avro',
      'Parquet',
      'CSV'
    ],
    answerIndex: 2,
    explanation: 'Parquet is columnar and optimized for queries that select specific columns, making it a strong choice for OLAP workloads.'
  },
  {
    id: 9,
    section: 'Data Ingestion & Source Control',
    question: 'What is the benefit of predicate pushdown in Spark?',
    options: [
      'It pushes Spark code to the driver process.',
      'It ensures all data is read before filtering.',
      'It applies filter conditions at the storage layer so fewer files are scanned.',
      'It converts all filters into UDFs.'
    ],
    answerIndex: 2,
    explanation: 'Predicate pushdown allows Spark to apply filters at the storage level, so only relevant files or blocks are read instead of scanning everything.'
  },
  {
    id: 10,
    section: 'Data Ingestion & Source Control',
    question: 'How does Spark optimize JDBC reads for large tables?',
    options: [
      'By using a single connection and sequential reads.',
      'By caching the entire JDBC table in memory.',
      'By using partitionColumn with lowerBound and upperBound for parallel reads.',
      'By writing JDBC query results to CSV first.'
    ],
    answerIndex: 2,
    explanation: 'Spark can read from JDBC sources in parallel by specifying partitionColumn, lowerBound, and upperBound so it executes multiple SELECTs over ranges.'
  },
  {
    id: 11,
    section: 'Transformation Logic & Performance',
    question: 'When does Spark use a broadcast join?',
    options: [
      'When both tables are extremely large.',
      'When the smaller table is sent to every executor to avoid a shuffle.',
      'When the join key is null.',
      'When the data is already bucketed by the join key.'
    ],
    answerIndex: 1,
    explanation: 'A broadcast join sends the small table to each executor, avoiding a shuffle. It can fail if the small table is too large for executor memory.'
  },
  {
    id: 12,
    section: 'Transformation Logic & Performance',
    question: 'What is the purpose of salting in a join strategy?',
    options: [
      'To compress the join keys before shuffle.',
      'To add a random prefix and distribute hot keys across partitions.',
      'To convert a broad transformation into a narrow transformation.',
      'To enforce schema on the join path.'
    ],
    answerIndex: 1,
    explanation: 'Salting adds a random prefix to the join key, helping distribute skewed hot keys more evenly across partitions and reducing task overload.'
  },
  {
    id: 13,
    section: 'Transformation Logic & Performance',
    question: 'Why are Python UDFs less efficient than Pandas UDFs?',
    options: [
      'Python UDFs cannot be executed in parallel.',
      'Python UDFs serialize each row between JVM and Python, while Pandas UDFs use Arrow batches.',
      'Pandas UDFs only work with RDDs, not DataFrames.',
      'Python UDFs are compiled into Java bytecode first.'
    ],
    answerIndex: 1,
    explanation: 'Python UDFs require per-row serialization/deserialization across JVM/Python boundaries. Pandas UDFs use Apache Arrow to transfer batches, making them far more efficient.'
  },
  {
    id: 14,
    section: 'Transformation Logic & Performance',
    question: 'Which Spark code creates a row number per user ordered by latest timestamp?',
    options: [
      'windowSpec = Window.partitionBy("user_id").orderBy(col("timestamp").asc())\ndf.withColumn("rank", row_number().over(windowSpec))',
      'windowSpec = Window.partitionBy("user_id")\ndf.withColumn("rank", rank().over(windowSpec))',
      'windowSpec = Window.partitionBy("user_id").orderBy(col("timestamp").desc())\ndf.withColumn("rank", row_number().over(windowSpec)).filter(col("rank") == 1)',
      'windowSpec = Window.orderBy(col("timestamp").desc())\ndf.withColumn("rank", row_number().over(windowSpec))'
    ],
    answerIndex: 2,
    explanation: 'The code uses partitionBy user_id and orderBy timestamp descending, then filters where rank == 1 to keep the latest row per user.'
  },
  {
    id: 15,
    section: 'Transformation Logic & Performance',
    question: 'What distinguishes narrow transformations from wide transformations in Spark?',
    options: [
      'Narrow operations always write to disk; wide operations are in-memory only.',
      'Narrow transformations keep data within the same partition, while wide transformations require a shuffle.',
      'Wide transformations are not allowed in Spark SQL.',
      'Narrow transformations create new stages in the DAG.'
    ],
    answerIndex: 1,
    explanation: 'Narrow transformations like map or filter do not require shuffling data; wide transformations like groupBy or join do, creating a new stage in the DAG.'
  },
  {
    id: 16,
    section: 'Data Sink & Output Management',
    question: 'What is a key difference between partitioning and bucketing?',
    options: [
      'Partitioning stores data in memory; bucketing stores it on disk.',
      'Partitioning creates physical folders; bucketing organizes data into fixed files by hash.',
      'Bucketing requires a Hive metastore; partitioning does not.',
      'Partitioning is only available for CSV output.'
    ],
    answerIndex: 1,
    explanation: 'Partitioning creates folder structures such as year=2023/, while bucketing splits data into fixed hash-based files for more efficient joins on the bucket column.'
  },
  {
    id: 17,
    section: 'Data Sink & Output Management',
    question: 'How do Delta Lake ACID transactions ensure partial writes are not visible to readers?',
    options: [
      'By using a transaction log that tracks valid files and versions.',
      'By always writing data to a temporary database before moving it.',
      'By committing all writes directly to HDFS without metadata.',
      'By creating a new table for each write.'
    ],
    answerIndex: 0,
    explanation: 'Delta Lake uses a transaction log to mark which file versions are valid, enabling atomic writes, time travel, and preventing partial data visibility.'
  },
  {
    id: 18,
    section: 'Data Sink & Output Management',
    question: 'What is the main difference between append and overwrite write modes?',
    options: [
      'append adds files while overwrite replaces the directory.',
      'append deletes previous data, overwrite keeps it.',
      'overwrite only works for streaming jobs.',
      'append requires bucketed output, overwrite does not.'
    ],
    answerIndex: 0,
    explanation: 'append adds new files to the existing dataset, while overwrite replaces the existing directory. In Delta/Iceberg, overwrite may rollback if it fails; standard Spark may leave corruption.'
  },
  {
    id: 19,
    section: 'Data Sink & Output Management',
    question: 'Which method reduces the number of output files without causing a full shuffle?',
    options: [
      'repartition(n)',
      'coalesce(n)',
      'cache()',
      'persist(StorageLevel.MEMORY_ONLY)'
    ],
    answerIndex: 1,
    explanation: 'coalesce(n) reduces the number of partitions without a full shuffle. repartition(n) performs a shuffle to rebalance data.'
  },
  {
    id: 20,
    section: 'Data Sink & Output Management',
    question: 'How can you implement an upsert into a Delta table in Spark?',
    options: [
      'df.write.mode("append").save(deltaTable)',
      'targetTable.alias("t").merge(sourceDF.alias("s"), "t.id = s.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()',
      'sourceDF.join(targetTable, "id").write.mode("overwrite").save("path")',
      'deltaTable.update(sourceDF)' 
    ],
    answerIndex: 1,
    explanation: 'A Delta merge with whenMatchedUpdateAll() and whenNotMatchedInsertAll() is the standard way to perform an upsert operation.'
  },
  {
    id: 21,
    section: 'Optimization & Debugging',
    question: 'What does a large difference between max task time and median task time in Spark UI usually indicate?',
    options: [
      'The cluster is running out of disk space.',
      'There is data skew in the workload.',
      'The Spark driver is in client mode.',
      'The job is using too many broadcast variables.'
    ],
    answerIndex: 1,
    explanation: 'A large max-to-median task time gap is often a sign of data skew, where a few partitions have much more work than the others.'
  },
  {
    id: 22,
    section: 'Optimization & Debugging',
    question: 'Which Spark plan view shows the actual execution details like FileScan and BroadcastHashJoin?',
    options: [
      'Logical Plan',
      'Optimized Logical Plan',
      'Physical Plan',
      'Source Plan'
    ],
    answerIndex: 2,
    explanation: 'The physical plan shows the actual execution operators that Spark will run, including operations like FileScan and BroadcastHashJoin.'
  },
  {
    id: 23,
    section: 'Optimization & Debugging',
    question: 'What is the difference between execution memory and storage memory in Spark?',
    options: [
      'Execution memory stores cached DataFrames; storage memory is for shuffles.',
      'Execution memory is for shuffles/joins and storage memory is for caching; they may borrow space if unused.',
      'They are completely separate and cannot borrow memory.',
      'Storage memory is only used for HDFS writes.'
    ],
    answerIndex: 1,
    explanation: 'Execution memory handles shuffles and joins while storage memory caches data; Spark can borrow memory across them when one side is underutilized.'
  },
  {
    id: 24,
    section: 'Optimization & Debugging',
    question: 'What is a drawback of Spark dynamic allocation?',
    options: [
      'It prevents Spark from releasing idle resources.',
      'It requires an external shuffle service to preserve shuffle files when executors die.',
      'It only works in local[1] mode.',
      'It disables caching entirely.'
    ],
    answerIndex: 1,
    explanation: 'Dynamic allocation can release executors when idle, but if an executor holding shuffle files dies, an external shuffle service is needed to preserve shuffle data.'
  },
  {
    id: 25,
    section: 'Optimization & Debugging',
    question: 'What is the purpose of a broadcast variable in Spark?',
    options: [
      'To broadcast a large DataFrame to all workers.',
      'To send a read-only lookup value to each executor efficiently.',
      'To cache RDD partitions on disk.',
      'To replace the Spark driver in cluster mode.'
    ],
    answerIndex: 1,
    explanation: 'A broadcast variable distributes a read-only value like a lookup mapping to all executors efficiently, avoiding repeated serialization in tasks.'
  },
  {
    id: 26,
    section: 'Testing, Security & CI/CD',
    question: 'What is a common pytest setup for Spark unit tests?',
    options: [
      'Create a SparkSession with master("local[1]") and reuse it across tests.',
      'Start a full YARN cluster for each test.',
      'Avoid using SparkSession and test only Python functions.',
      'Use only RDDs and not DataFrames.'
    ],
    answerIndex: 0,
    explanation: 'Using a local SparkSession with master("local[1]") in a pytest fixture is efficient and reusable for unit tests.'
  },
  {
    id: 27,
    section: 'Testing, Security & CI/CD',
    question: 'Why should Python logs be routed through Spark Log4j?',
    options: [
      'So Spark can translate them into SQL statements.',
      'So Python logs use the same JVM logging pipeline and cluster logging settings.',
      'Because Python cannot log directly to stdout in Spark.',
      'To automatically encrypt all logs.'
    ],
    answerIndex: 1,
    explanation: 'Routing Python logs through Spark Log4j ensures they follow the same logging pipeline and can be collected consistently by cluster logging infrastructure.'
  },
  {
    id: 28,
    section: 'Testing, Security & CI/CD',
    question: 'How does Great Expectations fit into a Spark pipeline?',
    options: [
      'It replaces Spark SQL entirely.',
      'It defines expectations for data quality as a validation step.',
      'It automatically tunes Spark memory settings.',
      'It only works with streaming pipelines.'
    ],
    answerIndex: 1,
    explanation: 'Great Expectations can define rules such as expect_column_values_to_not_be_null and run validation as part of a pipeline to enforce data quality.'
  },
  {
    id: 29,
    section: 'Testing, Security & CI/CD',
    question: 'What is the recommended way to manage secrets for Spark jobs?',
    options: [
      'Hardcode credentials in the Spark application source code.',
      'Store secrets in service-specific vaults and access them through config or environment variables.',
      'Embed secrets in DataFrame metadata.',
      'Use plain text files in HDFS.'
    ],
    answerIndex: 1,
    explanation: 'Secrets should be stored in vaults like AWS Secrets Manager or Azure Key Vault, then accessed securely through configuration or mounted environment variables.'
  },
  {
    id: 30,
    section: 'Testing, Security & CI/CD',
    question: 'How can you distribute Python dependencies to Spark executors?',
    options: [
      'Use --py-files to send .zip or .py archives, or spark.archives for Conda/Venv bundles.',
      'Install packages manually on each executor node only.',
      'Use only system Python on the driver machine.',
      'Copy dependencies into the Spark UI application description.'
    ],
    answerIndex: 0,
    explanation: 'Spark supports --py-files for .zip/.py distribution and spark.archives for transporting Conda/Venv environments to executors.'
  }
];

const sections = Array.from(new Set(questions.map(q => q.section)));

function initFilter() {
  sections.forEach(section => {
    const option = document.createElement('option');
    option.value = section;
    option.textContent = section;
    sectionFilter.appendChild(option);
  });
}

function renderQuiz(filter = 'all') {
  quizContainer.innerHTML = '';
  const filteredQuestions = filter === 'all' ? questions : questions.filter(q => q.section === filter);

  filteredQuestions.forEach(question => {
    const card = document.createElement('article');
    card.className = 'quiz-card';

    card.innerHTML = `
      <div class="question-meta">
        <span>Question ${question.id}</span>
        <span>${question.section}</span>
      </div>
      <div class="question-text">${question.question}</div>
      <div class="options"></div>
      <div class="card-actions">
        <button type="button" class="check-btn">Check Answer</button>
        <button type="button" class="show-btn">Show Model Answer</button>
      </div>
      <div class="answer-feedback" hidden>
        <div class="result"></div>
        <div class="explanation"></div>
      </div>
    `;

    const optionsContainer = card.querySelector('.options');
    question.options.forEach((optionText, index) => {
      const optionLabel = document.createElement('label');
      optionLabel.className = 'option';
      optionLabel.innerHTML = `
        <input type="radio" name="question-${question.id}" value="${index}" />
        <span>${optionText}</span>
      `;
      optionsContainer.appendChild(optionLabel);
    });

    const resultBox = card.querySelector('.answer-feedback');
    const resultText = card.querySelector('.result');
    const explanationText = card.querySelector('.explanation');

    card.querySelector('.check-btn').addEventListener('click', () => {
      const selected = card.querySelector(`input[name="question-${question.id}"]:checked`);
      if (!selected) {
        resultText.textContent = 'Please choose an option before checking.';
        resultText.className = 'result answer-wrong';
        explanationText.textContent = '';
        resultBox.hidden = false;
        return;
      }

      const selectedIndex = Number(selected.value);
      const isCorrect = selectedIndex === question.answerIndex;
      resultText.textContent = isCorrect ? 'Correct!' : `Incorrect. The correct answer is ${String.fromCharCode(65 + question.answerIndex)}.`;
      resultText.className = `result ${isCorrect ? 'answer-correct' : 'answer-wrong'}`;
      explanationText.textContent = question.explanation;
      resultBox.hidden = false;
    });

    card.querySelector('.show-btn').addEventListener('click', () => {
      resultText.textContent = `Answer: ${String.fromCharCode(65 + question.answerIndex)}.`;
      resultText.className = 'result answer-correct';
      explanationText.textContent = question.explanation;
      resultBox.hidden = false;
    });

    quizContainer.appendChild(card);
  });
}

sectionFilter.addEventListener('change', () => renderQuiz(sectionFilter.value));
resetButton.addEventListener('click', () => {
  document.querySelectorAll('input[type=radio]').forEach(input => input.checked = false);
  document.querySelectorAll('.answer-feedback').forEach(box => { box.hidden = true; });
});

initFilter();
renderQuiz();
