package k8s

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// 获取命名空间
func GetNamespaceByName(clientset *kubernetes.Clientset, namespaceName string) (namespace *corev1.Namespace, err error) {
	namespace, err = clientset.CoreV1().Namespaces().Get(context.TODO(), namespaceName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return namespace, nil
}

// 创建命名空间
func CreateNamespace(clientset *kubernetes.Clientset, namespaceName string, namespace *corev1.Namespace) (createNamespace *corev1.Namespace, err error) {
	_, err = GetNamespaceByName(clientset, namespaceName)
	if err != nil {
		if errors.IsNotFound(err) {
			createNamespace, err = clientset.CoreV1().Namespaces().Create(context.TODO(), namespace, metav1.CreateOptions{})
		}
	}
	return
}
