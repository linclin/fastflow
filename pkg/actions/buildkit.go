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
	ActionKeyBUILDKIT = "buildkit"
)

type BUILDKITParams struct {
	GitUrl         string            `json:"git-url"`
	GitKey         string            `json:"git-key"`
	BuildArg       map[string]string `json:"build-arg"`
	Image          string            `json:"image"`
	Version        string            `json:"version"`
	RegistrySecret string            `json:"registry-secret"`
	Cluster        string            `json:"cluster"`
	Namespace      string            `json:"namespace"`
}

type BUILDKIT struct {
}

func (s *BUILDKIT) Name() string {
	return ActionKeyBUILDKIT
}

// ParameterNew
func (s *BUILDKIT) ParameterNew() interface{} {
	return &BUILDKITParams{}
}

// Run
func (s *BUILDKIT) Run(ctx run.ExecuteContext, params interface{}) (err error) {
	ctx.Trace("[Action buildkit]start " + fmt.Sprintln("params", params))
	p, ok := params.(*BUILDKITParams)
	if !ok {
		err = fmt.Errorf("[Action buildkit]params type mismatch, want *BUILDKITParams, got %T", params)
		ctx.Trace(err.Error())
		return err
	}
	if p.Cluster == "" || p.Namespace == "" {
		err = fmt.Errorf("[Action buildkit]BUILDKITParams cluster/namespace  cannot be empty")
		ctx.Trace(err.Error())
		return err
	}
	dir, err := os.Getwd()
	if err != nil {
		err = fmt.Errorf("[Action buildkit]Getwd error" + err.Error())
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
			err = fmt.Errorf("[Action go-git]git-url error" + err.Error())
			ctx.Trace(err.Error())
			return err
		}
		gitPath = url.Hostname() + strings.Replace(url.Path, ".git", "", -1)
	}
	_, err = os.Stat(storagePath + "buildkit-cache/" + gitPath)
	if err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(storagePath+"buildkit-cache/"+gitPath, os.ModePerm)
		}
	}
	jobNmae := "buildkit-" + cast.ToString(time.Now().UnixNano())
	buildkitJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobNmae,
			Namespace: p.Namespace,
			Labels: map[string]string{
				"job": jobNmae,
			},
			Annotations: map[string]string{
				"container.apparmor.security.beta.kubernetes.io/buildkit-build-job": "unconfined",
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            lo.ToPtr(int32(0)),
			ActiveDeadlineSeconds:   lo.ToPtr(int64(600)),
			TTLSecondsAfterFinished: lo.ToPtr(int32(10)),
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
							Name:            "buildkit",
							Image:           "registry.cn-shenzhen.aliyuncs.com/dev-ops/buildkit:v0.12.1-rootless",
							ImagePullPolicy: corev1.PullAlways,
							Env: []corev1.EnvVar{
								{Name: "BUILDKITD_FLAGS", Value: "--oci-worker-no-process-sandbox"},
							},
							WorkingDir: "/workspace/git-source/" + gitPath,
							Command:    []string{"buildctl-daemonless.sh"},
							Args: []string{
								"build",
								"--frontend", "dockerfile.v0",
								"--local", "context=.",
								"--local", "dockerfile=.",
								"--output", "type=image,name=" + p.Image + ":" + p.Version + ",push=true",
								"--export-cache", "type=local,mode=max,dest=/workspace/buildkit-cache/" + gitPath,
								"--import-cache", "type=local,src=/workspace/buildkit-cache/" + gitPath,
								"--export-cache", "type=registry,mode=max,ref=" + p.Image + ":buildkit-cache",
								"--import-cache", "type=registry,ref=" + p.Image + ":buildkit-cache",
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "docer-secret",
									MountPath: "/home/user/.docker",
								},
								{
									Name:      "workspace",
									MountPath: "/workspace",
								},
							},
							SecurityContext: &corev1.SecurityContext{
								RunAsUser:  lo.ToPtr(int64(1000)),
								RunAsGroup: lo.ToPtr(int64(1000)),
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
		for k, v := range p.BuildArg {
			buildkitJob.Spec.Template.Spec.Containers[0].Args = append(buildkitJob.Spec.Template.Spec.Containers[0].Args, "--opt")
			buildkitJob.Spec.Template.Spec.Containers[0].Args = append(buildkitJob.Spec.Template.Spec.Containers[0].Args, "build-arg:"+k+"="+v)
		}
	}
	k8sClient, err := k8s.InitK8SClient(p.Cluster)
	if err != nil {
		ctx.Trace("[Action buildkit]InitK8SClient " + err.Error())
		return err
	}
	_, err = k8s.CreateUpdateJob(k8sClient, p.Namespace, jobNmae, buildkitJob)
	if err != nil {
		ctx.Trace("[Action buildkit]CreateUpdateJob " + err.Error())
		return err
	}
	lo.AttemptWhileWithDelay(60, 10*time.Second, func(i int, d time.Duration) (error, bool) {
		success, resons, err := k8s.GetJobStatus(k8sClient, p.Namespace, jobNmae)
		ctx.Trace("[Action buildkit]执行中 " + strings.Join(resons, "\n"))
		if err != nil {
			ctx.Trace("[Action buildkit]build error " + err.Error())
			return err, false
		} else {
			if success {
				ctx.Trace("[Action buildkit]build success ")
				return nil, false
			} else {
				return fmt.Errorf("获取Job[%s]的Pod状态执行中", "buildkit"), true
			}
		}
	})
	ctx.Trace("[Action buildkit]success " + fmt.Sprintln("params", params))
	return nil
}
