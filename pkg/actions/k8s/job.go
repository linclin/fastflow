package k8s

import (
	"context"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

// 获取命名空间下的指定Job
func GetJobByName(clientset *kubernetes.Clientset, namespace, jobName string) (job *batchv1.Job, err error) {
	job, err = clientset.BatchV1().Jobs(namespace).Get(context.TODO(), jobName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return job, nil
}

// 创建或更新指定Job
func CreateUpdateJob(clientset *kubernetes.Clientset, namespace, jobName string, job *batchv1.Job) (editJob *batchv1.Job, err error) {
	_, err = GetJobByName(clientset, namespace, jobName)
	if err != nil {
		if errors.IsNotFound(err) {
			editJob, err = clientset.BatchV1().Jobs(namespace).Create(context.TODO(), job, metav1.CreateOptions{})
		}
	} else {
		editJob, err = clientset.BatchV1().Jobs(namespace).Update(context.TODO(), job, metav1.UpdateOptions{})
	}
	if !errors.IsAlreadyExists(err) {
		return editJob, err
	}
	return editJob, nil
}

// 获取命名空间下的指定Job运行状态
func GetJobStatus(clientset *kubernetes.Clientset, namespace, jobName string) (success bool, reasons []string, err error) {
	job, joberr := GetJobByName(clientset, namespace, jobName)
	if joberr != nil {
		reasons = append(reasons, fmt.Sprintf("获取Job[%s]状态错误%s", jobName, joberr.Error()))
		err = fmt.Errorf("获取Job[%s]状态错误%s", jobName, joberr.Error())
		return
	}
	if len(job.Status.Conditions) > 0 {
		if job.Status.Conditions[0].Status == "True" && job.Status.Conditions[0].Type == "Complete" && job.Spec.Completions == &job.Status.Succeeded && job.Status.CompletionTime != nil {
			success = true
			return
		}
		reasons = append(reasons, fmt.Sprintf("获取Job[%s]状态%s", jobName, job.Status.Conditions[0].Type))
	}
	matchLabels := make(map[string]string)
	matchLabels["job"] = jobName
	labelselector := metav1.LabelSelector{
		MatchLabels:      matchLabels,
		MatchExpressions: nil,
	}
	labelMap, labelErr := metav1.LabelSelectorAsMap(&labelselector)
	if labelErr != nil {
		err = fmt.Errorf("获取Job[%s]的Pod失败，原因为 %s", jobName, labelErr.Error())
		return
	}
	// 获取job下的所有pod
	podList, plErr := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: labels.SelectorFromSet(labelMap).String()})
	if plErr != nil {
		if !errors.IsNotFound(plErr) {
			err = fmt.Errorf("获取Job[%s]的Pod失败，原因为 %s", jobName, plErr.Error())
			return
		} else {
			success = true
			return
		}
	}
	for _, pod := range podList.Items {
		//获取pod状态
		if pod.Status.Phase == corev1.PodFailed {
			err = fmt.Errorf("获取Job[%s]的Pod[%s]状态错误%s，退出执行", jobName, pod.Name, pod.Status.Phase)
			return
		}
		//获取pod事件
		podMatchLabels := make(map[string]string)
		podMatchLabels["involvedObject.kind"] = "Pod"
		podMatchLabels["involvedObject.name"] = pod.Name
		podLabelSelector := metav1.LabelSelector{MatchLabels: podMatchLabels, MatchExpressions: nil}
		fieldMap, fielderr := metav1.LabelSelectorAsMap(&podLabelSelector)
		if fielderr == nil {
			eventList, eventerr := clientset.CoreV1().Events(namespace).List(context.TODO(), metav1.ListOptions{FieldSelector: fields.SelectorFromSet(fieldMap).String()})
			if eventerr == nil {
				warnEvent := make(map[string]string)
				for _, event := range eventList.Items {
					if event.Type != "Normal" {
						warnEvent[event.Reason] = event.Message
					}
				}
				for r, m := range warnEvent {
					reasons = append(reasons, fmt.Sprintf("获取Job[%s]Pod名称[%s]，事件类型: %s，事件信息: %s", jobName, pod.Name, r, m))
				}
			}
		}
		for _, containerStatus := range pod.Status.ContainerStatuses {
			if containerStatus.State.Waiting != nil {
				if strings.Contains(containerStatus.State.Waiting.Reason, "Err") || strings.Contains(containerStatus.State.Waiting.Reason, "BackOff") || strings.Contains(containerStatus.State.Waiting.Reason, "Invalid") || strings.Contains(containerStatus.State.Waiting.Reason, "Unavailable") {
					err = fmt.Errorf("获取Job[%s]的Pod[%s]状态错误%s，退出执行", jobName, pod.Name, containerStatus.State.Waiting)
					return
				}
			}
		}
	}

	//获取pod日志
	return
}
