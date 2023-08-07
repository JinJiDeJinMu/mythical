package com.jm.mythical.k8s.operator.spark.deployment;

import cn.hutool.core.collection.CollectionUtil;
import com.jm.mythical.k8s.operator.spark.deployment.context.SparkContext;
import com.jm.mythical.k8s.operator.spark.deployment.context.SparkDeployMode;
import com.jm.mythical.k8s.spark.operator.crds.RestartPolicy;
import com.jm.mythical.k8s.spark.operator.crds.SparkApplication;
import com.jm.mythical.k8s.spark.operator.crds.SparkApplicationSpec;
import com.jm.mythical.k8s.spark.operator.crds.SparkPodSpec;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionList;
import io.fabric8.kubernetes.client.CustomResource;

import java.util.List;
import java.util.stream.Collectors;


/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/8/3 16:44
 */
public class OperatorKubernetesClusterDeployment extends AbstractKubernetesClusterDeployment<SparkContext> {

    KubernetesClientAdapter kubernetesClientAdapter;

    SparkApplication sparkApplication;

    public OperatorKubernetesClusterDeployment(SparkContext sparkContext) {
        super(sparkContext);
    }

    @Override
    public void preSubmit() {
        kubernetesClientAdapter = new KubernetesClientAdapter(
                sparkConfig.getK8sMasterUrl(),
                sparkConfig.getK8sCarCertData(),
                sparkConfig.getK8sClientCrtData(),
                sparkConfig.getK8sClientKeyData()
        );

        if (CollectionUtil.isEmpty(checkSparkOperatorIsExist())) {
            throw new RuntimeException("spark operator not exist");
        }

        sparkApplication = getSparkApplication();

        SparkPodSpec driver =
                SparkPodSpec.Builder()
                        .cores(sparkConfig.getDriverCores())
                        .memory(sparkConfig.getDriverMemory())
                        .serviceAccount(sparkConfig.getK8sServiceAccount())
                        .build();

        SparkPodSpec executor =
                SparkPodSpec.Builder()
                        .cores(sparkConfig.getExecutorCores())
                        .instances(sparkConfig.getNumExecutors())
                        .memory(sparkConfig.getExecutorMemory())
                        .build();

        SparkApplicationSpec sparkApplicationSpec =
                SparkApplicationSpec.Builder()
                        .type(sparkConfig.getK8sLanguageType())
                        .mode(SparkDeployMode.CLUSTER.name())
                        .image(sparkConfig.getK8sImage())
                        .imagePullPolicy(sparkConfig.getK8sImagePullPolicy())
                        .mainClass(sparkConfig.getMainClass())
                        .mainApplicationFile(sparkConfig.getAppResource())
                        .arguments(sparkConfig.getArgs())
                        .sparkVersion(sparkConfig.getK8sSparkVersion())
                        .restartPolicy(new RestartPolicy(sparkConfig.getK8sRestartPolicy()))
                        .driver(driver)
                        .executor(executor)
                        .sparkConf(sparkConfig.getConf())
                        .build();

        sparkApplication.setSpec(sparkApplicationSpec);
    }

    @Override
    public void doSubmit() {
        SparkApplication application = this.kubernetesClientAdapter.getKubernetesClient().resource(sparkApplication).create();

        /**
         * 监控是否创建成功
         * 1.k8s自带的list-watch
         * 2.sleep几秒之后再查询
         */

    }

    @Override
    protected void postSubmit() {
        this.kubernetesClientAdapter.closeKubernetesClient();
    }


    @Override
    public void init() {
        if (p.checkParameters()) {
            throw new RuntimeException("参数校验不通过");
        }
        sparkConfig = p.getSparkConfig();
    }

    @Override
    public void cancel() {
        List<StatusDetails> statusDetails = this.kubernetesClientAdapter.getKubernetesClient().resource(getSparkApplication()).delete();

        //todo 是否需要判断停止成功
        this.kubernetesClientAdapter.closeKubernetesClient();
    }

    public SparkApplication getSparkApplication() {
        SparkApplication sparkApplication = new SparkApplication();
        ObjectMeta metadata = new ObjectMeta();
        metadata.setName(sparkConfig.getAppName());
        metadata.setNamespace(sparkConfig.getK8sNamespace());
        sparkApplication.setMetadata(metadata);
        return sparkApplication;
    }

    public List<CustomResourceDefinition> checkSparkOperatorIsExist() {
        CustomResourceDefinitionList customResourceDefinitionList =
                this.kubernetesClientAdapter.getKubernetesClient().apiextensions().v1().customResourceDefinitions().list();

        String sparkApplicationCRDName = CustomResource.getCRDName(SparkApplication.class);
        return customResourceDefinitionList.getItems().stream()
                .filter(crd -> crd.getMetadata().getName().equals(sparkApplicationCRDName))
                .collect(Collectors.toList());
    }
}
