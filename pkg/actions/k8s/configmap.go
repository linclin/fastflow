package k8s

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// 获取Configmap
func GetConfigmap(clientset *kubernetes.Clientset, namespace, configmapName string) (configMap *corev1.ConfigMap, err error) {
	configMap, err = clientset.CoreV1().ConfigMaps(namespace).Get(context.TODO(), configmapName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return configMap, nil
}

// 获取Configmap列表
func GetConfigmapList(clientset *kubernetes.Clientset, namespace string, labelSelector string) (configMapList *corev1.ConfigMapList, err error) {
	configMapList, err = clientset.CoreV1().ConfigMaps(namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return nil, err
	}
	return configMapList, nil
}

// 创建或更新指定Configmap
func CreateUpdateConfigmap(clientset *kubernetes.Clientset, namespace, configmapName string, configmap *corev1.ConfigMap) (configMap *corev1.ConfigMap, err error) {
	_, err = GetConfigmap(clientset, namespace, configmapName)
	if err != nil {
		if errors.IsNotFound(err) {
			configMap, err = clientset.CoreV1().ConfigMaps(namespace).Create(context.TODO(), configmap, metav1.CreateOptions{})
		}
	} else {
		configMap, err = clientset.CoreV1().ConfigMaps(namespace).Update(context.TODO(), configmap, metav1.UpdateOptions{})
	}
	return
}

func UpdateConfigmap(clientset *kubernetes.Clientset, namespace string, configmap *corev1.ConfigMap) (configMap *corev1.ConfigMap, err error) {
	configMap, err = clientset.CoreV1().ConfigMaps(namespace).Update(context.TODO(), configmap, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	return
}
