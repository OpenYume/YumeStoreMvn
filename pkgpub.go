package main

import (
	"bufio"
	"bytes"
	"flag"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"

	"fvvlang.sbs/fvv"
)

type Config struct {
	Domain string        `fvv:"Domain,omitempty"`
	Repos  []*RepoConfig `fvv:"Repos"`
}

type RepoConfig struct {
	Url    string   `fvv:"Url"`
	Dir    string   `fvv:"Dir,omitempty"`
	Ignore []string `fvv:"Ignore,omitempty"`
}

type State struct {
	Repos []*RepoState `fvv:"Repos"`
}

type RepoState struct {
	Url  string     `fvv:"Url"`
	Tags []*TagInfo `fvv:"Tags"`
}

type TagInfo struct {
	Tag  string `fvv:"Tag"`
	Hash string `fvv:"Hash"`
}

type BuildTask struct {
	Config RepoConfig
	Tag    string
	Hash   string
	Index  int
}

var (
	cfg_file    = flag.String("config", "maven.fvv", "config file path")
	out_dir     = flag.String("output", "dist-maven", "output directory")
	check_only  = flag.Bool("check", false, "only check")
	domain_file string
	state_file  string
	err         error
)

func main() {
	flag.Parse()
	domain_file = filepath.Join(*out_dir, "CNAME")
	state_file = filepath.Join(*out_dir, "maven.fvv")

	if err = os.MkdirAll(*out_dir, os.ModePerm); err != nil {
		log.Fatalf("cannot `os.MkdirAll`: %s", err.Error())
	}

	var tgt_cfg *Config
	raw_cfg, err := os.ReadFile(*cfg_file)
	if err != nil {
		log.Fatalf("cannot `os.ReadFile`: %s", err.Error())
	}
	cfg_fwv := fvv.NewFVVV()
	if err = cfg_fwv.ParseString(string(raw_cfg)); err != nil {
		log.Fatalf("cannot `fvv.ParseString`: %s", err.Error())
	} else if err = cfg_fwv.Unmarshal(&tgt_cfg); err != nil {
		log.Fatalf("cannot `fvv.Unmarshal`: %s", err.Error())
	}

	var tgt_state *State
	raw_state, err := os.ReadFile(state_file)
	if err == nil {
		if err = fvv.NewFVVV().ParseString(string(raw_state), &tgt_state); err != nil {
			log.Fatalf("cannot `fvv.ParseString`: %s", err.Error())
		}
	}
	if tgt_state == nil {
		tgt_state = &State{Repos: []*RepoState{}}
	}
	state_map := make(map[string]*RepoState)
	for _, repo := range tgt_state.Repos {
		state_map[repo.Url] = repo
	}

	var tasks []BuildTask
	var tasks_lock sync.Mutex
	scan_ctx_cancel := make(chan struct{})
	scan_sem := make(chan struct{}, runtime.NumCPU())
	var scan_wg sync.WaitGroup
	for idx, idx_cfg := range tgt_cfg.Repos {
		scan_wg.Add(1)
		go func(index int, repo_cfg *RepoConfig) {
			defer scan_wg.Done()
			select {
			case <-scan_ctx_cancel:
				return
			case scan_sem <- struct{}{}:
			}
			defer func() { <-scan_sem }()

			var ignore_regs []*regexp.Regexp
			for _, pattern := range repo_cfg.Ignore {
				reg, err := regexp.Compile(pattern)
				if err != nil {
					log.Printf("cannot `regexp.Compile`(%s): %s", pattern, err.Error())
					continue
				}
				ignore_regs = append(ignore_regs, reg)
			}

			cmd := exec.Command("git", "ls-remote", "--tags", repo_cfg.Url)
			out, err := cmd.Output()
			if err != nil {
				log.Printf("cannot `exec.Command`(git ls-remote --tags %s): %s",
					repo_cfg.Url, err.Error())
				return
			}

			remote_tags := make(map[string]string)
			scanner := bufio.NewScanner(bytes.NewReader(out))
			for scanner.Scan() {
				parts := strings.Fields(scanner.Text())
				if len(parts) < 2 {
					continue
				}
				hash, tag := parts[0], parts[1]

				if !strings.HasPrefix(tag, "refs/tags/") {
					continue
				}
				tag = strings.TrimPrefix(tag, "refs/tags/")

				if tag, ok := strings.CutSuffix(tag, "^{}"); ok {
					remote_tags[tag] = hash
				} else if _, found := remote_tags[tag]; !found {
					remote_tags[tag] = hash
				}
			}

			local_tags := make(map[string]string)
			if repo_state, found := state_map[repo_cfg.Url]; found {
				for _, info := range repo_state.Tags {
					local_tags[info.Tag] = info.Hash
				}
			}

			for tag, hash := range remote_tags {
				ignored := false
				for _, reg := range ignore_regs {
					if reg.MatchString(tag) {
						ignored = true
						break
					}
				}
				if ignored {
					continue
				}

				old_hash, found := local_tags[tag]
				if !found || old_hash != hash {
					if *check_only {
						os.Exit(0)
					}
					log.Printf("added: %s @ %s", repo_cfg.Url, tag)
					tasks_lock.Lock()
					tasks = append(tasks, BuildTask{
						Config: *repo_cfg,
						Tag:    tag,
						Hash:   hash,
						Index:  index,
					})
					tasks_lock.Unlock()
				}
			}
		}(idx, idx_cfg)
	}
	scan_wg.Wait()
	if *check_only {
		os.Exit(-1)
	}

	if tgt_cfg.Domain != "" {
		if err = os.WriteFile(domain_file, []byte(tgt_cfg.Domain), os.ModePerm); err != nil {
			log.Printf("cannot `os.WriteFile`: %s", err.Error())
		}
	}

	if len(tasks) == 0 {
		log.Printf("nothing to do")
		return
	}
	sort.Slice(tasks, func(a, b int) bool {
		return tasks[a].Index < tasks[b].Index
	})

	home_dir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("cannot `os.UserHomeDir`: %s", err.Error())
	}
	local_m2 := filepath.Join(home_dir, ".m2", "repository")
	_ = os.RemoveAll(local_m2)
	if err = filepath.Walk(*out_dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		name := info.Name()
		if name == ".git" {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if name == "CNAME" || name == "maven.fvv" {
			return nil
		}

		rel, err := filepath.Rel(*out_dir, path)
		if err != nil {
			return err
		}
		dst_path := filepath.Join(local_m2, rel)
		if name == "maven-metadata.xml" {
			dst_path = filepath.Join(filepath.Dir(dst_path), "maven-metadata-local.xml")
		}

		if info.IsDir() {
			return os.MkdirAll(dst_path, info.Mode())
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		src_file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src_file.Close()
		dst_file, err := os.Create(dst_path)
		if err != nil {
			return err
		}
		defer dst_file.Close()
		if _, err := io.Copy(dst_file, src_file); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatalf("cannot `filepath.Walk`: %s", err.Error())
	}

	for _, task := range tasks {
		log.Printf("building: %s -> %s", task.Config.Url, task.Tag)

		work_dir, _ := os.MkdirTemp("", ".pkgpub-*")

		clone_cmd := exec.Command("git", "clone", "--depth", "1", "--recurse-submodules",
			"--branch", task.Tag, task.Config.Url, work_dir)
		clone_cmd.Stdout = os.Stdout
		clone_cmd.Stderr = os.Stderr
		if err = clone_cmd.Run(); err != nil {
			log.Printf("cannot `exec.Command`(git clone --branch %s %s): %s",
				task.Tag, task.Config.Url, err.Error())
			_ = os.RemoveAll(work_dir)
			continue
		}

		if task.Config.Dir != "" {
			work_dir = filepath.Join(work_dir, task.Config.Dir)
		}

		_ = os.Chmod(filepath.Join(work_dir, "gradlew"), os.ModePerm)
		build_cmd := exec.Command(filepath.Join(work_dir, "gradlew"), "clean", "assemble", "publishToMavenLocal")
		build_cmd.Dir = work_dir
		build_cmd.Stdout = os.Stdout
		build_cmd.Stderr = os.Stderr
		if err = build_cmd.Run(); err != nil {
			log.Printf("cannot `exec.Command`(gradlew publishToMavenLocal): %s", err.Error())
			_ = os.RemoveAll(work_dir)
			continue
		}
		_ = os.RemoveAll(work_dir)

		repo_state, found := state_map[task.Config.Url]
		if !found {
			repo_state = &RepoState{Url: task.Config.Url, Tags: []*TagInfo{}}
			tgt_state.Repos = append(tgt_state.Repos, repo_state)
			state_map[task.Config.Url] = repo_state
		}

		tag_found := false
		for _, info := range repo_state.Tags {
			if info.Tag == task.Tag {
				info.Hash = task.Hash
				tag_found = true
				break
			}
		}
		if !tag_found {
			repo_state.Tags = append(repo_state.Tags, &TagInfo{Tag: task.Tag, Hash: task.Hash})
		}

		fwv := fvv.NewFVVV()
		if err = fwv.Marshal(tgt_state); err != nil {
			log.Printf("cannot `fvv.Marshal`: %s", err.Error())
		} else if err = os.WriteFile(state_file, []byte(fwv.ToString(fvv.FmtOptMinify)), os.ModePerm); err != nil {
			log.Printf("cannot `os.WriteFile`: %s", err.Error())
		}
	}

	if _, err = os.Stat(local_m2); err != nil {
		return
	}
	if err = filepath.Walk(local_m2, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(local_m2, path)
		if err != nil {
			return err
		}
		dst_path := filepath.Join(*out_dir, rel)
		if info.Name() == "maven-metadata-local.xml" {
			dst_path = filepath.Join(filepath.Dir(dst_path), "maven-metadata.xml")
		}

		if info.IsDir() {
			return os.MkdirAll(dst_path, info.Mode())
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		src_file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src_file.Close()
		dst_file, err := os.Create(dst_path)
		if err != nil {
			return err
		}
		defer dst_file.Close()
		if _, err := io.Copy(dst_file, src_file); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatalf("cannot `filepath.Walk`: %s", err.Error())
	}
}
