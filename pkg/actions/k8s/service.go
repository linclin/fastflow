package k8s

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// 获取命名空间下的指定Service
func GetServiceByName(clientset *kubernetes.Clientset, namespace, serviceName string) (service *corev1.Service, err error) {
	service, err = clientset.CoreV1().Services(namespace).Get(context.TODO(), serviceName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return service, nil
}

// 创建或更新指定Service
func CreateUpdateService(clientset *kubernetes.Clientset, namespace, serviceName string, service *corev1.Service) (createService *corev1.Service, err error) {
	_, err = GetServiceByName(clientset, namespace, serviceName)
	if err != nil {
		if errors.IsNotFound(err) {
			createService, err = clientset.CoreV1().Services(namespace).Create(context.TODO(), service, metav1.CreateOptions{})
		}
	} else {
		//createService, err = clientset.CoreV1().Services(namespace).Update(context.TODO(), service, metav1.UpdateOptions{})
	}
	return
}
