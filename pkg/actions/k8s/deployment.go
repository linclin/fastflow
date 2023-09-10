package k8s

import (
	"context"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// 获取命名空间下的指定Deployment
func GetDeployment(clientset *kubernetes.Clientset, namespace, deploymentName string) (deployment *appsv1.Deployment, err error) {
	deployment, err = clientset.AppsV1().Deployments(namespace).Get(context.TODO(), deploymentName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return
}

// 创建或更新指定Deployment
func CreateUpdateDeployment(clientset *kubernetes.Clientset, namespace, deploymentName string, deployment *appsv1.Deployment) (createDeployment *appsv1.Deployment, err error) {
	_, err = GetDeployment(clientset, namespace, deploymentName)
	if err != nil {
		if errors.IsNotFound(err) {
			createDeployment, err = clientset.AppsV1().Deployments(namespace).Create(context.TODO(), deployment, metav1.CreateOptions{})
		}
	} else {
		createDeployment, err = clientset.AppsV1().Deployments(namespace).Update(context.TODO(), deployment, metav1.UpdateOptions{})
	}
	return
}

func UpdateDeployment(clientset *kubernetes.Clientset, namespace string, deployment *appsv1.Deployment) (updateDeployment *appsv1.Deployment, err error) {
	updateDeployment, err = clientset.AppsV1().Deployments(namespace).Update(context.TODO(), deployment, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	return
}

func DeleteDeployment(clientset *kubernetes.Clientset, namespace, deploymentName string) (err error) {
	deletePolicy := metav1.DeletePropagationForeground
	err = clientset.AppsV1().Deployments(namespace).Delete(context.TODO(), deploymentName, metav1.DeleteOptions{PropagationPolicy: &deletePolicy})
	if err != nil {
		return err
	}
	return
}

// 获取命名空间下的指定Deployment状态
func GetDeploymentStatus(clientset *kubernetes.Clientset, namespace, deploymentName string) (success bool, reasons []string, err error) {
	// 获取deployment
	deployment, err := GetDeployment(clientset, namespace, deploymentName)
	if err != nil {
		return false, []string{"获取Deployment状态错误"}, err
	}
	labelSelector := ""
	for key, value := range deployment.Spec.Selector.MatchLabels {
		labelSelector = labelSelector + key + "=" + value + ","
	}
	labelSelector = strings.TrimRight(labelSelector, ",")
	// 获取deployment下的所有pod
	podList, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return false, []string{"获取Pod状态错误"}, err
	}
	readyPod := 0
	unavailablePod := 0
	waitingReasons := []string{}
	for _, pod := range podList.Items {
		// 记录等待原因
		for _, containerStatus := range pod.Status.ContainerStatuses {
			if containerStatus.State.Waiting != nil {
				reason := "pod " + pod.Name + ", container " + containerStatus.Name + ", waiting reason: " + containerStatus.State.Waiting.Reason
				waitingReasons = append(waitingReasons, reason)
			}
		}
		podScheduledCondition := GetPodCondition(pod.Status, corev1.PodScheduled)
		initializedCondition := GetPodCondition(pod.Status, corev1.PodInitialized)
		readyCondition := GetPodCondition(pod.Status, corev1.PodReady)
		containersReadyCondition := GetPodCondition(pod.Status, corev1.ContainersReady)
		if pod.Status.Phase == "Running" &&
			podScheduledCondition.Status == "True" &&
			initializedCondition.Status == "True" &&
			readyCondition.Status == "True" &&
			containersReadyCondition.Status == "True" {
			readyPod++
		} else {
			unavailablePod++
		}
	}
	// 根据container状态判定
	if len(waitingReasons) != 0 {
		return false, waitingReasons, nil
	}
	// 根据pod状态判定
	if int32(readyPod) < *(deployment.Spec.Replicas) ||
		int32(unavailablePod) != 0 {
		return false, []string{"Pod不是ready状态"}, nil
	}
	// deployment进行状态判定
	availableCondition := GetDeploymentCondition(deployment.Status, appsv1.DeploymentAvailable)
	progressingCondition := GetDeploymentCondition(deployment.Status, appsv1.DeploymentProgressing)

	if deployment.Status.UpdatedReplicas != *(deployment.Spec.Replicas) ||
		deployment.Status.Replicas != *(deployment.Spec.Replicas) ||
		deployment.Status.AvailableReplicas != *(deployment.Spec.Replicas) ||
		availableCondition.Status != "True" ||
		progressingCondition.Status != "True" {
		return false, []string{"Deployments不是ready状态"}, nil
	}

	if deployment.Status.ObservedGeneration < deployment.Generation {
		return false, []string{"Deployments状态错误"}, nil
	}
	// 发布成功
	return true, []string{}, nil
}

// GetDeploymentCondition returns the condition with the provided type.
func GetDeploymentCondition(status appsv1.DeploymentStatus, condType appsv1.DeploymentConditionType) *appsv1.DeploymentCondition {
	for i := range status.Conditions {
		c := status.Conditions[i]
		if c.Type == condType {
			return &c
		}
	}
	return nil
}

func GetPodCondition(status corev1.PodStatus, condType corev1.PodConditionType) *corev1.PodCondition {
	for i := range status.Conditions {
		c := status.Conditions[i]
		if c.Type == condType {
			return &c
		}
	}
	return nil
}
