package k8s

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// 获取命名空间下的指定POD
func GetPodsByName(clientset *kubernetes.Clientset, namespace, podName string) (pods *corev1.Pod, err error) {
	pods, err = clientset.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return pods, nil
}

// 获取Pod日志
func GetContainerLog(clientset *kubernetes.Clientset, namespace, podName, containerName string) (podsLogs string, err error) {
	// 获取日志请求
	req := clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{Container: containerName})
	// req.Stream()也可以实现Do的效果
	// 发送请求
	res := req.Do(context.TODO())
	if res.Error() != nil {
		fmt.Println(res.Error())
		return
	}
	// 获取结果
	logs, err := res.Raw()
	if err != nil {
		return "", err
	}
	podsLogs = string(logs)
	return podsLogs, err
}
