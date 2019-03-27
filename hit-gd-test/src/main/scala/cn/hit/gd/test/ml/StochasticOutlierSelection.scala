package cn.hit.gd.test.ml

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.outlier.StochasticOutlierSelection
import org.apache.flink.api.scala._
import org.apache.flink.table.sources.CsvTableSource
import org.scalatest._
import org.junit.{Before, Test}


/**
  * @author: create by ywrao
  * @version: v1.0
  * @description: cn.hit.gd.test.ml
  * @date:2019 /1/26
  **/
/**
  * An outlier is one or multiple observations that deviates quantitatively from the majority of the data set and may be the subject of further investigation.
  * Stochastic Outlier Selection (SOS) developed by Jeroen Janssens[1] is an unsupervised outlier-selection algorithm that takes as input a set of vectors.
  * The algorithm applies affinity-based outlier selection and outputs for each data point an outlier probability.
  * Intuitively, a data point is considered to be an outlier when the other data points have insufficient affinity with it.
  * 离群值是一个或多个观测值，从数量上偏离大多数数据集，可能是进一步调查的主题。
  * Jeroen Janssens[1]开发的随机离群值选择（SoS）是一种无监督的离群值选择算法，它以一组向量作为输入。
  * 该算法应用基于关联的离群值选择，并为每个数据点输出离群值概率。直观地说，当其他数据点与某个数据点没有足够的相关性时，该数据点被认为是离群值。
  * 离群值检测在许多领域都有应用，如日志分析、欺诈检测、噪声消除、新颖性检测、质量控制、传感器监测等。如果传感器出现故障，很可能会输出明显偏离大多数值的值。
  * Outlier detection has its application in a number of field,
  * for example, log analysis, fraud detection, noise removal, novelty detection, quality control, sensor monitoring, etc.
  * If a sensor turns faulty, it is likely that it will output values that deviate markedly from the majority
  * */
class StochasticOutlierSelectionTest {

  val env = ExecutionEnvironment.getExecutionEnvironment

  @Before
  def befor(): Unit = {
    env.setParallelism(1)
  }

  @Test
  def test1(): Unit = {

    val data = env.fromCollection(List(
      LabeledVector(0.0, DenseVector(1.0, 1.0)),
      LabeledVector(1.0, DenseVector(2.0, 1.0)),
      LabeledVector(2.0, DenseVector(1.0, 2.0)),
      LabeledVector(3.0, DenseVector(2.0, 2.0)),
      LabeledVector(4.0, DenseVector(5.0, 8.0)) // The outlier!
    ))
    data.print()

    val sos = new StochasticOutlierSelection().setPerplexity(3)

    sos.transform(data).print()

//    val outputVector = sos
//      .transform(data)
//      .collect()
//
//    val expectedOutputVector = Map(
//      0 -> 0.2790094479202896,
//      1 -> 0.25775014551682535,
//      2 -> 0.22136130977995766,
//      3 -> 0.12707053787018444,
//      4 -> 0.9922779902453757 // The outlier!
//    )

//    outputVector.foreach(output => expectedOutputVector(output._1) should be(output._2))
  }

  @Test
  def test2(): Unit ={
    val INPUT_FILE_PATH="file:///"+this.getClass.getResource("sz000001_20181204.csv");
//    val rowTypeInfo=new RowTypeInfo(new TypeInformation[] {Types.STRING},new String[]{"security_code"})
//    val data=CsvTableSource.builder.path(INPUT_FILE_PATH).field(rowTypeInfo.getFieldNames,rowTypeInfo.getFieldTypes).getDataSet()
  }
}
