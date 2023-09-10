package actions

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"github.com/linclin/fastflow/pkg/entity/run"
	gssh "golang.org/x/crypto/ssh"
)

const (
	ActionKeyGOGIT = "go-git"
)

type GOGITParams struct {
	GitUrl      string `json:"git-url"`
	GitBranch   string `json:"git-branch"`
	GitKey      string `json:"git-key"`
	GitUsername string `json:"git-username"`
	GitPassword string `json:"git-password"`
	GitToken    string `json:"git-token"`
}

type GOGIT struct {
}

func (g *GOGIT) Name() string {
	return ActionKeyGOGIT
}

// ParameterNew
func (g *GOGIT) ParameterNew() interface{} {
	return &GOGITParams{}
}

// Run
func (g *GOGIT) Run(ctx run.ExecuteContext, params interface{}) (err error) {
	ctx.Trace("[Action go-git]start " + fmt.Sprintln("params", params))
	p, ok := params.(*GOGITParams)
	if !ok {
		err = fmt.Errorf("[Action go-git]params type mismatch, want *GOGITParams, got %T", params)
		ctx.Trace(err.Error())
		return err
	}
	if p.GitUrl == "" || p.GitBranch == "" {
		err = fmt.Errorf("[Action go-git]GOGITParams git-url/branch cannot be empty")
		ctx.Trace(err.Error())
		return err
	}
	storagePath := "./storage/git-source/"
	gitPath := ""
	if strings.Contains(p.GitUrl, "git@") {
		gitPath = strings.Replace(strings.Replace(strings.Replace(p.GitUrl, "git@", "", -1), ":", "/", -1), ".git", "", -1)
	} else {
		url, err := url.Parse(p.GitUrl)
		if err != nil {
			err = fmt.Errorf("[Action go-git]GOGITParams git-url error" + err.Error())
			ctx.Trace(err.Error())
			return err
		}
		gitPath = url.Hostname() + strings.Replace(url.Path, ".git", "", -1)
	}
	var gitAuth transport.AuthMethod
	if p.GitKey != "" {
		publicKeys, err := ssh.NewPublicKeysFromFile("git", "./storage/git-key/"+p.GitKey, "")
		if err != nil {
			ctx.Trace("[Action go-git] ssh key error " + err.Error())
			return err
		}
		publicKeys.HostKeyCallback = gssh.InsecureIgnoreHostKey()
		gitAuth = publicKeys
	} else if p.GitToken != "" {
		gitAuth = &http.BasicAuth{
			Username: p.GitUsername, // yes, this can be anything except an empty string
			Password: p.GitToken,
		}
	} else if p.GitUsername != "" && p.GitPassword != "" {
		gitAuth = &http.BasicAuth{
			Username: p.GitUsername,
			Password: p.GitPassword,
		}
	} else {
		err = fmt.Errorf("[Action go-git]GOGITParams auth required")
		ctx.Trace(err.Error())
		return err
	}
	if _, err := os.Stat(storagePath + gitPath + "/.git"); os.IsNotExist(err) {
		_, err = git.PlainClone(storagePath+gitPath, false, &git.CloneOptions{
			URL:           p.GitUrl,
			Auth:          gitAuth,
			ReferenceName: plumbing.NewBranchReferenceName(p.GitBranch),
			Progress:      os.Stdout,
		})
		if err != nil {
			ctx.Trace("[Action go-git]git clone failed " + err.Error())
			return err
		}
	} else {
		repository, err := git.PlainOpen(storagePath + gitPath)
		if err != nil {
			ctx.Trace("[Action go-git]PlainOpen failed " + err.Error())
			return err
		}
		worktree, err := repository.Worktree()
		if err != nil {
			ctx.Trace("[Action go-git]Worktree failed " + err.Error())
			return err
		}
		err = worktree.Pull(&git.PullOptions{
			Auth:          gitAuth,
			ReferenceName: plumbing.NewBranchReferenceName(p.GitBranch),
			Progress:      os.Stdout})
		if err != nil && err != git.NoErrAlreadyUpToDate {
			ctx.Trace("[Action go-git]git pull failed " + err.Error())
			return err
		}
	}
	ctx.Trace("[Action go-git]success " + fmt.Sprintln("params", params))
	return nil
}
