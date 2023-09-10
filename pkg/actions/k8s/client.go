package k8s

import (
	"os"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// 初始化k8s客户端
func InitK8SClient(clusterName string) (clientset *kubernetes.Clientset, err error) {
	var restConf *rest.Config
	if restConf, err = GetK8SRestConf(clusterName); err != nil {
		return
	}
	// 生成clientset配置
	if clientset, err = kubernetes.NewForConfig(restConf); err != nil {
		goto END
	}
END:
	return
}

// 获取k8s restful client配置
func GetK8SRestConf(clusterName string) (restConf *rest.Config, err error) {
	var kubeconfig []byte
	kubeconfigfile := "./storage/kubeconfig/" + clusterName
	// 读kubeconfig文件
	if kubeconfig, err = os.ReadFile(kubeconfigfile); err != nil {
		goto END
	}
	// 生成rest client配置
	if restConf, err = clientcmd.RESTConfigFromKubeConfig(kubeconfig); err != nil {
		goto END
	}
END:
	return
}
