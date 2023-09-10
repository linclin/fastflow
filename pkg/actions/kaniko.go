package actions

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/linclin/fastflow/pkg/actions/k8s"
	"github.com/linclin/fastflow/pkg/entity/run"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ActionKeyKANIKO = "kaniko"
)

type KANIKOParams struct {
	GitUrl         string            `json:"git-url"`
	GitBranch      string            `json:"git-branch"`
	GitUsername    string            `json:"git-username"`
	GitPassword    string            `json:"git-password"`
	GitToken       string            `json:"git-token"`
	BuildArg       map[string]string `json:"build-arg"`
	Image          string            `json:"image"`
	Version        string            `json:"version"`
	RegistrySecret string            `json:"registry-secret"`
	Cluster        string            `json:"cluster"`
	Namespace      string            `json:"namespace"`
}

type KANIKO struct {
}

func (s *KANIKO) Name() string {
	return ActionKeyKANIKO
}

// ParameterNew
func (s *KANIKO) ParameterNew() interface{} {
	return &KANIKOParams{}
}

// Run
func (s *KANIKO) Run(ctx run.ExecuteContext, params interface{}) (err error) {
	ctx.Trace("[Action kaniko]start " + fmt.Sprintln("params", params))
	p, ok := params.(*KANIKOParams)
	if !ok {
		err = fmt.Errorf("[Action kaniko]params type mismatch, want *BUILDKITParams, got %T", params)
		ctx.Trace(err.Error())
		return err
	}
	if p.Cluster == "" || p.Namespace == "" {
		err = fmt.Errorf("[Action kaniko]BUILDKITParams cluster/namespace  cannot be empty")
		ctx.Trace(err.Error())
		return err
	}
	dir, err := os.Getwd()
	if err != nil {
		err = fmt.Errorf("[Action kaniko]Getwd error" + err.Error())
		ctx.Trace(err.Error())
		return err
	}
	storagePath := dir + "/storage/"
	gitPath := ""
	if strings.Contains(p.GitUrl, "git@") {
		gitPath = strings.Replace(strings.Replace(strings.Replace(p.GitUrl, "git@", "", -1), ":", "/", -1), ".git", "", -1)
	} else {
		url, err := url.Parse(p.GitUrl)
		if err != nil {
			err = fmt.Errorf("[Action kaniko] git-url error" + err.Error())
			ctx.Trace(err.Error())
			return err
		}
		gitPath = url.Hostname() + strings.Replace(url.Path, ".git", "", -1)
	}
	_, err = os.Stat(storagePath + "kaniko-cache/" + gitPath)
	if err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(storagePath+"kaniko-cache/"+gitPath, os.ModePerm)
		}
	}
	jobNmae := "kaniko-" + cast.ToString(time.Now().UnixNano())
	buildkitJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobNmae,
			Namespace: p.Namespace,
			Labels: map[string]string{
				"job": jobNmae,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            lo.ToPtr(int32(0)),
			ActiveDeadlineSeconds:   lo.ToPtr(int64(600)),
			TTLSecondsAfterFinished: lo.ToPtr(int32(600)),
			Completions:             lo.ToPtr(int32(1)),
			ManualSelector:          lo.ToPtr(true),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"job": jobNmae,
				},
				MatchExpressions: nil,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: jobNmae,
					Labels: map[string]string{
						"job": jobNmae,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:            "kaniko",
							Image:           "registry.cn-shenzhen.aliyuncs.com/dev-ops/kaniko-executor:v1.15.0",
							ImagePullPolicy: corev1.PullAlways,
							Env: []corev1.EnvVar{
								{Name: "GIT_USERNAME", Value: p.GitUsername},
							},
							Args: []string{
								"--context=" + p.GitUrl,
								"--git=branch=" + p.GitBranch,
								"--git=single-branch=true",
								//"--context=/workspace/",
								//"--dockerfile=/workspace/Dockerfile",
								"--destination=" + p.Image + ":" + p.Version,
								"--cache",
								"--cache-dir=kaniko-cache/" + gitPath,
								"--cache-repo=" + p.Image + "-kaniko-cache",
								"--cache-copy-layers",
								"--cache-run-layers",
								"--push-retry=2",
								"--verbosity=debug",
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "docer-secret",
									MountPath: "/kaniko/.docker",
								},
								{
									Name:      "workspace",
									MountPath: "/workspace",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "docer-secret",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: p.RegistrySecret,
									Items: []corev1.KeyToPath{
										{Key: ".dockerconfigjson", Path: "config.json"},
									},
								},
							},
						},
						{
							Name: "workspace",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: storagePath,
								},
							},
						},
					},
				},
			},
		},
	}
	if len(p.BuildArg) > 0 {
		buildkitJob.Spec.Template.Spec.Containers[0].Args = append(buildkitJob.Spec.Template.Spec.Containers[0].Args, "--build-arg")
		for k, v := range p.BuildArg {
			buildkitJob.Spec.Template.Spec.Containers[0].Args = append(buildkitJob.Spec.Template.Spec.Containers[0].Args, k+"="+v+" ")
		}
	}
	if p.GitPassword != "" {
		buildkitJob.Spec.Template.Spec.Containers[0].Env = append(buildkitJob.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "GIT_PASSWORD", Value: p.GitPassword})
	}
	if p.GitToken != "" {
		buildkitJob.Spec.Template.Spec.Containers[0].Env = append(buildkitJob.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "GIT_TOKEN", Value: p.GitToken})
	}
	k8sClient, err := k8s.InitK8SClient(p.Cluster)
	if err != nil {
		ctx.Trace("[Action kaniko]InitK8SClient " + err.Error())
		return err
	}
	_, err = k8s.CreateUpdateJob(k8sClient, p.Namespace, jobNmae, buildkitJob)
	if err != nil {
		ctx.Trace("[Action kaniko]CreateUpdateJob " + err.Error())
		return err
	}
	lo.AttemptWhileWithDelay(60, 10*time.Second, func(i int, d time.Duration) (error, bool) {
		success, resons, err := k8s.GetJobStatus(k8sClient, p.Namespace, jobNmae)
		ctx.Trace("[Action kaniko]执行中 " + strings.Join(resons, "\n"))
		if err != nil {
			ctx.Trace("[Action kaniko]build error " + err.Error())
			return err, false
		} else {
			if success {
				ctx.Trace("[Action kaniko]build success ")
				return nil, false
			} else {
				return fmt.Errorf("获取Job[%s]的Pod状态执行中", "kaniko"), true
			}
		}
	})
	ctx.Trace("[Action kaniko]success " + fmt.Sprintln("params", params))
	return nil
}
